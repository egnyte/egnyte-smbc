import os
import sys
import copy
import errno
import signal
import logging

from time import time, sleep
from multiprocessing import Process

from smb.smb_list import SmbList
from smb.smb_xattr import SmbXAttr
from smb.smb_read import SmbRead
from smb.smb_write import SmbWrite
from smb.smb import (SmbPermissionsException, SmbObjectNotFoundException,
                     SmbTimedOutException, SmbConnectionException,
                     InvalidSmbConfigException, SmbNotaDirectoryException,
                     SmbDepthExceededException, SmbNotaFileException,
                     SmbWriteException, HEARTBEAT_WAIT)

from config import ConfigManager
from config import (ConfigNotAvailableException, ConfigNotChangedException,
                    ConfigNotFoundException, InvalidConfigException,
                    ConfigException)

# Seconds to wait before retrying the connection
RETRY_CONNECTION_WAIT_TIME = 2

# Etcd location for the global config
GLOBAL_CONFIG = "/hub/global/ad"

_logger = logging.getLogger(__name__)

# Add except hook to log uncaught exceptions instead of outputting it to stderr
def log_uncaught_exception(exc_type, exc_value, exc_tb):
    _logger.error("UNCAUGHT EXCEPTION", exc_info=(exc_type, exc_value, exc_tb))

sys.excepthook = log_uncaught_exception


class WorkerResetException(Exception):
    """ Exception raised to indicate that the worker must be reset
        i.e. drop current job immediately
    """
    def __init__(self, message=None):
        super(WorkerResetException, self).__init__(message)


class WorkerShutdownException(Exception):
    """ Exception raised to indicate that the worker must be shutdown now """
    def __init__(self, message=None):
        super(WorkerShutdownException, self).__init__(message)


def mask_password(config):
    """ Returns a copy of config with the password masked out """

    # Sanity check: config should be a dict, skip masking if not dict
    if not isinstance(config, dict):
        return config

    masked_config = copy.deepcopy(config)
    if 'service_password' in masked_config:
        masked_config['service_password'] = '***********'

    return masked_config


def sanitize_path(path):
    """ Returns a posix style path """
    posix_style_path = path.replace('\\\\', '/')
    posix_style_path = posix_style_path.replace('\\', '/')
    return posix_style_path.rstrip("/")


class PoolWorker(Process):

    def __init__(self):
        super(PoolWorker, self).__init__()
        self.stop = False
        self.ppid = None
        self.current_job = None
        self.current_job_id = None
        self.last_heartbeat = None
        self.current_job_status = None
        self.config = ConfigManager()

    def run(self):
        # Ignore SIGTERM, install signal handlers for SIGINT and SIGHUP
        signal.signal(signal.SIGTERM, signal.SIG_IGN)
        signal.signal(signal.SIGINT, self.handle_shutdown)
        signal.signal(signal.SIGHUP, self.handle_reset)
        self.ppid = os.getppid()

        _logger.info("Worker process was started by the pool manager with pid %s", self.ppid)
        while not self.stop:
            try:
                self.send_heartbeat()
                for new_job in self.config.watch_for_available_jobs():
                    self.send_heartbeat()

                    # Could get an empty result while watching for changes
                    # This is an etcd issue so have to deal with it
                    if not new_job:
                        continue

                    # Pick the first entry
                    job_id = new_job.keys()[0]

                    # Watches return responses for deletes too
                    # Don't process jobs deleted from the available queue
                    if not new_job[job_id]:
                        continue

                    try:
                        # Claim the job
                        self.config.claim_available_job(job_id)
                    except ConfigNotFoundException:
                        # Job was claimed by another worker, go back to waiting
                        _logger.info("Could not claim job %s from the available queue", job_id)
                        continue

                    self.current_job_id = job_id
                    self.config.update_job_started(self.current_job_id)

                    # Get the smb specific job information
                    temp_config = self.config.get_job_config(job_id)

                    new_job_config = {}
                    new_job_config["type"] = temp_config["type"]
                    for key in ("share", "depth", "folder", "filter", "file_path", "junction", "volume"):
                        if key in temp_config:
                            new_job_config[key] = temp_config[key]
                    new_job_config["jobid"] = job_id

                    # Get source configuration info from the global directory
                    global_config = self.config.get_source_config(GLOBAL_CONFIG)
                    # Get source configuration info from the provided source path
                    source_config = self.config.get_source_config(temp_config["source_path"])

                    # Populate job config with source information
                    # Specific source config takes priority over global config
                    for key in ("service_user", "service_password", "domain", "host"):
                        if key in source_config:
                            new_job_config[key] = source_config[key]
                        elif key in global_config:
                            new_job_config[key] = global_config[key]

                    # Sanitize values that could store file system like paths
                    for key in ("share", "folder", "file_path"):
                        if key in new_job_config:
                            new_job_config[key] = sanitize_path(new_job_config[key])

                    _logger.info("Running job with id %s and configuration as %r", self.current_job_id, mask_password(new_job_config))

                    try:
                        self.config.increment_job_try_count(self.current_job_id)
                    except ConfigException:
                        # Log and continue, this is not a fatal error
                        _logger.exception("Could not increment try count for job %s", self.current_job_id)

                    # Get the appropriate smb type object
                    if new_job_config["type"] == "list":
                        self.current_job = SmbList(new_job_config, self.config)
                    elif new_job_config["type"] == "xattr":
                        self.current_job = SmbXAttr(new_job_config, self.config)
                    elif new_job_config["type"] == "read":
                        self.current_job = SmbRead(new_job_config, self.config)
                    else:
                        self.current_job = SmbWrite(new_job_config, self.config)

                    self.config.update_process_jobid(self.pid, self.current_job_id)
                    self.current_job.execute()
                    self.cleanup(self.current_job_status)
            except ConfigNotChangedException:
                self.send_heartbeat()
            except KeyError as e:
                message = "%s not found in the config" % str(e)
                _logger.exception(message)
                self.current_job_status = "INVALID_CONFIG: " + message
                if not self.current_job:
                    self.cleanup(self.current_job_status, True)
            except InvalidConfigException:
                message = "Invalid config provided"
                _logger.exception(message)
                self.current_job_status = "INVALID_CONFIG: " + message
                if not self.current_job:
                    self.cleanup(self.current_job_status, True)
            except ConfigNotFoundException:
                message = "Config not found"
                _logger.exception(message)
                self.current_job_status = "INVALID_CONFIG: " + message
                if not self.current_job:
                    self.cleanup(self.current_job_status, True)
            except ConfigNotAvailableException:
                message = "Network error while getting the config"
                _logger.exception(message)
                self.current_job_status = "UNKNOWN_ERROR: " + message
                if not self.current_job:
                    self.cleanup(self.current_job_status, True)
                sleep(RETRY_CONNECTION_WAIT_TIME)
            except ConfigException:
                message = "Error getting the config"
                _logger.exception(message)
                self.current_job_status = "UNKNOWN_ERROR: " + message
                if not self.current_job:
                    self.cleanup(self.current_job_status, True)
                sleep(RETRY_CONNECTION_WAIT_TIME)
            except InvalidSmbConfigException as e:
                _logger.exception("Job %s has an invalid configuration", self.current_job_id)
                self.current_job_status = "SMB_INVALID_CONFIG: " + e.message
                self.cleanup(self.current_job_status, True)
            except SmbDepthExceededException as e:
                _logger.exception("Exceeded maximum depth while performing smb operation for job %s", self.current_job_id)
                self.current_job_status = "SMB_INVALID_CONFIG: " + e.message
            except SmbPermissionsException as e:
                _logger.exception("Incorrect permissions for job %s", self.current_job_id)
                self.current_job_status = "SMB_INSUFFICIENT_PERMS: " + e.message
            except SmbObjectNotFoundException as e:
                _logger.exception("Could not find object for job %s", self.current_job_id)
                self.current_job_status = "SMB_NOT_FOUND: " + e.message
            except SmbTimedOutException as e:
                _logger.exception("Smb operation timed out for job %s", self.current_job_id)
                self.current_job_status = "SMB_NOT_AVAILABLE: " + e.message
            except SmbNotaDirectoryException as e:
                _logger.exception("Expected directory is not a dir for job %s", self.current_job_id)
                self.current_job_status = "SMB_INVALID_CONFIG: " + e.message
            except SmbNotaFileException as e:
                _logger.exception("Expected file is not a file for job %s", self.current_job_id)
                self.current_job_status = "SMB_INVALID_CONFIG: " + e.message
            except SmbConnectionException as e:
                _logger.exception("Connection error for job %s", self.current_job_id)
                self.current_job_status = "UNKNOWN_ERROR: " + e.message
            except SmbWriteException as e:
                _logger.exception("Write error for job %s", self.current_job_id)
                self.current_job_status = "UNKNOWN_ERROR: " + e.message
            except IOError as e:
                # Ignore if EINTR, otherwise raise
                if e.errno != errno.EINTR:
                    raise
            except WorkerResetException:
                if self.current_job_id:
                    _logger.info("Worker will reset: stopped job with id %s", self.current_job_id)
                else:
                    _logger.info("Worker will reset")
            except WorkerShutdownException:
                if self.current_job_id:
                    _logger.info("Worker will shutdown: stopped job with id %s", self.current_job_id)
                else:
                    _logger.info("Worker will shutdown")
                self.stop = True
            finally:
                self.cleanup(self.current_job_status)

        _logger.info("Worker process was stopped")

    def cleanup(self, job_status, invalid_config=False):
        """ Completes the post execute steps
            Updates the relevant config items, cleans up resources

            This method runs if the job was completed, interrupted or could not
            be run because of an invalid configuration
        """
        job_status = job_status or "Success"

        if invalid_config and self.current_job_id:
            _logger.info("Invalid configuration, cleaning up after job %s", self.current_job_id)
            try:
                self.config.update_job_completed(self.current_job_id)
            except ConfigException:
                _logger.exception("Could not set completed timestamp for job %s", self.current_job_id)

            # Update job status
            try:
                self.config.set_job_status(self.current_job_id, job_status)
            except ConfigException:
                _logger.exception("Could not set status for job %s", self.current_job_id)

            try:
                self.config.increment_process_fail_count(self.pid)
            except ConfigException:
                _logger.exception("Error incrementing the process fail count")
        elif self.current_job:
            _logger.info("Cleaning up after job %s", self.current_job_id)
            output_file = self.current_job.get_output_file_name()

            try:
                self.config.update_job_completed(self.current_job_id)
            except ConfigException:
                _logger.exception("Could not set completed timestamp for job %s", self.current_job_id)

            if job_status == "Success" and self.current_job.smb_config["type"] != "write":
                try:
                    self.config.update_job_output(self.current_job_id, output_file)
                except ConfigException:
                    _logger.exception("Could not update output data for job %s", self.current_job_id)
                    # Change job status to failure
                    job_status = "UNKNOWN_ERROR: Could not update output field in the job config"

            # Update job status in the config regardless of Success or Failure
            try:
                self.config.set_job_status(self.current_job_id, job_status)
            except ConfigException:
                _logger.exception("Could not set status for job %s", self.current_job_id)

            self.current_job.close()
            # In case the job was interrupted, explicitly call del to
            # indicate that resources can be freed
            del self.current_job

            # Reset jobid for this worker process
            try:
                self.config.update_process_jobid(self.pid, " ")
            except ConfigException:
                _logger.exception("Could not reset jobid for process %s, previous job executed was %s", self.pid, self.current_job_id)

            try:
                if job_status != "Success":
                    # Worker failed to execute current job
                    self.config.increment_process_fail_count(self.pid)
                else:
                    self.config.reset_process_fail_count(self.pid)
            except ConfigException:
                _logger.exception("Error updating the config")

        self.current_job = None
        self.current_job_id = None
        self.current_job_status = None

    def handle_shutdown(self, signum, frame):
        """ Handler for SIGINT received from the pool manager, initiates
            worker shutdown
        """
        raise WorkerShutdownException("Shutdown the worker now")

    def handle_reset(self, signum, frame):
        """ Handler for SIGHUP received from the pool manager, initiates
            worker reset, i.e. drop the current job
        """
        raise WorkerResetException("Reset the worker now")

    def send_heartbeat(self):
        """ Post hearbeat information using the configuration manager
            Throttled to once every HEARTBEAT_WAIT seconds
        """
        send = True
        now = long(time())
        if self.last_heartbeat and (now - self.last_heartbeat) < HEARTBEAT_WAIT:
            send = False
        if send:
            self.config.update_process_heartbeat(self.pid)
            self.last_heartbeat = now
