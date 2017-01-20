""" Main module for the smbc service.
    This module contains the following:
    1) WorkerMonitor class which is a thread that is spawned for each
       worker process. It monitors the worker by watching its config space for
       heartbeats, assigned job changes and fail counts
    2) PoolManager class which manages the lifecycle of pool workers.
       It starts and stops workers and associated monitor threads.
       It also orchestrates a "clean" shutdown when it receives a SIGTERM

    The process tree for this service is depicted in the following lines.
    Processes are in upper case, threads in lower case.

        |------- run (WorkerMonitor)
        |------- POOL WORKER
        |   .
        |   .
        |   MIN_WORKERS <= n <= MAX_WORKERS
        |   .
        |   .
        |------- POOL WORKER
        |------- run (WorkerMonitor)
        |
    POOL MANAGER
        |----- receive (MultiPlexedLog)
"""

import os
import sys
import errno
import signal
import logging
import threading
import logging.config

from time import time, sleep
from multiprocessing import active_children

import yaml

from config import ConfigManager
from config import (ConfigNotAvailableException, InvalidConfigException,
                    ConfigException)
from pool_worker import (PoolWorker, RETRY_CONNECTION_WAIT_TIME)
from server import SmbcServer

MIN_WORKERS = 4
MAX_WORKERS = 8

# Seconds to wait for process(es) to quit
PROCESS_WAIT_TIME = 5

_logger = logging.getLogger(__name__)


# Add except hook to log uncaught exceptions and shutdown gracefully
def log_uncaught_exception(exc_type, exc_value, exc_tb):
    _logger.error("UNCAUGHT EXCEPTION", exc_info=(exc_type, exc_value, exc_tb))
    logging.shutdown()
    sleep(PROCESS_WAIT_TIME)

sys.excepthook = log_uncaught_exception


class WorkerMonitor(threading.Thread):
    """ Thread class which monitors the worker with process id pid """
    def __init__(self, pid, pool_manager, proc):
        super(WorkerMonitor, self).__init__()
        self.pid = pid
        self.proc = proc
        self.quit = False
        self.pmanager = pool_manager
        self.config = ConfigManager()
        self.worker_state = {'heartbeat' : str(long(time())), 'successive_fail_count' : '0', 'jobid' : ' '}

    def run(self):
        """ Monitors worker's heartbeat and assigned jobid posted in the config
        """
        _logger.info("Monitor thread started for worker with pid %s", self.pid)
        while not self.quit:
            try:
                for changes in self.config.watch_process_config(self.pid):
                    if not changes:
                        continue
                    if self.quit:
                        break
                    for x in ("heartbeat", "successive_fail_count", "jobid"):
                        if x in changes and self.worker_state[x] != changes[x]:
                            self.worker_state[x] = changes[x]
                            if x == "jobid":
                                if self.is_idle():
                                    self.pmanager.increment_idle_workers()
                                    _logger.info("Worker with pid %s is now idle", self.pid)
                                else:
                                    self.pmanager.decrement_idle_workers()
                                    _logger.info("Worker with pid %s is now busy", self.pid)
            except ConfigNotAvailableException:
                _logger.exception("Network error watching config for worker with pid %s", self.pid)
                sleep(RETRY_CONNECTION_WAIT_TIME)
            except ConfigException:
                _logger.exception("Error watching config for worker with pid %s", self.pid)
                sleep(RETRY_CONNECTION_WAIT_TIME)
        _logger.info("Monitor thread stopped for worker with pid %s", self.pid)

    def terminate(self):
        self.quit = True

    def is_worker_alive(self):
        return self.proc.is_alive()

    def is_idle(self):
        return self.worker_state["jobid"].isspace()

    def should_reset(self):
        """ Returns True if the heartbeat is older than 5 mins or for
            exceptions, False otherwise """
        try:
            heartbeat = long(self.worker_state["heartbeat"])
        except ValueError:
            _logger.error("Malformed hearbeat value %s found for worker with pid %s", self.worker_state["heartbeat"], self.pid)
            return True
        return long(time()) - heartbeat >= 300

    def should_shutdown(self):
        """ Returns True if the successive fail count has exceeded 5 """
        try:
            if int(self.worker_state["successive_fail_count"]) < 5:
                return False
        except ValueError:
            _logger.error("Malformed fail count %s found for worker with pid %s", self.worker_state["successive_fail_count"], self.pid)
        return True


class PoolManager(object):
    """ Manages the worker process pool
        Communicates with workers using signals
    """
    def __init__(self):
        self.monitors = {}       # Map of worker pids to monitor thread objects
        self.idle_workers = 0
        self.total_workers = 0
        self.shutdown = threading.Event()
        self.smbc_server_proc = None
        self.lock = threading.Lock()
        self.config = ConfigManager()

    def handle_shutdown(self, signum, frame):
        """ Handler for SIGTERM, sets shutdown event """
        self.shutdown.set()

    def increment_idle_workers(self):
        """ Increment idle worker count once the lock is acquired """
        with self.lock:
            self.idle_workers += 1
            _logger.info("Idle workers: %s, Total workers: %s", self.idle_workers, self.total_workers)

    def decrement_idle_workers(self):
        """ Decrement idle worker count once the lock is acquired """
        with self.lock:
            self.idle_workers -= 1
            _logger.info("Idle workers: %s, Total workers: %s", self.idle_workers, self.total_workers)

    def reset_idle_workers(self):
        """ Reset the idle worker count once the lock is acquired """
        with self.lock:
            self.idle_workers = 0

    def get_idle_workers(self):
        """ Returns the idle workers count """
        with self.lock:
            return self.idle_workers

    def send_shutdown(self, pid):
        """ Sends SIGINT to the worker with pid to shut it down, worker should
            stop cleanly when it receives this signal
        """
        if pid in self.monitors:
            if self.monitors[pid].is_worker_alive():
                _logger.info("Shutting down worker process with pid %s", pid)
                os.kill(pid, signal.SIGINT)
            else:
                _logger.warning("Shutdown error: Process with pid %s is already stopped", pid)
        else:
            _logger.error("Process with pid %s was not found", pid)

    def send_reset(self, pid):
        """ Sends SIGHUP to the worker with pid to reset it, making it stop
            processing the current job
        """
        if pid in self.monitors:
            if self.monitors[pid].is_worker_alive():
                _logger.info("Resetting worker process with pid %s", pid)
                os.kill(pid, signal.SIGHUP)
            else:
                _logger.warning("Reset error: Process with pid %s is stopped", pid)
        else:
            _logger.error("Process with pid %s was not found", pid)

    def start_smbc_server(self):
        """ Starts a new http server which exposes pool worker like
            functionality for synchronous calls
        """
        self.smbc_server_proc = SmbcServer()
        self.smbc_server_proc.daemon = True
        self.smbc_server_proc.start()
        _logger.info("Started smbc server process with pid %s", self.smbc_server_proc.pid)

    def stop_smbc_server(self):
        """ Stops the smbc server process if it is running """
        if self.is_smbc_server_alive():
            self.smbc_server_proc.terminate()
            _logger.info("Stopped smbc server process with pid %s", self.smbc_server_proc.pid)
        else:
            _logger.warning("smbc server process is already stopped")

    def is_smbc_server_alive(self):
        """ Returns True if the smbc server is alive, False otherwise """
        alive = False
        if self.smbc_server_proc:
            alive = self.smbc_server_proc.is_alive()
        return alive

    def start_worker(self):
        """ Starts a new worker process, initializes shared state, and updates
            the internal worker map
            Also starts a new thread which will monitor the worker's
            heartbeat and assigned jobid using the configuration manager
        """
        proc = PoolWorker()
        proc.daemon = True
        proc.start()
        pid = proc.pid

        _logger.info("Started worker process with pid %s", pid)

        self.total_workers += 1
        self.increment_idle_workers()

        # Create process config tree, terminate process on failure
        try:
            # Start health monitoring thread
            mthread = WorkerMonitor(pid, self, proc)
            mthread.daemon = True
            mthread.start()
            self.monitors[pid] = mthread

            self.config.create_process_config(pid)
            _logger.info("Created process config for worker with pid %s", pid)
        except ConfigNotAvailableException:
            _logger.exception("Network error while creating config for worker with pid %s", pid)
            self.stop_worker(pid, is_idle=True)
        except ConfigException:
            _logger.exception("Error creating config for worker with pid %s", pid)
            self.stop_worker(pid, is_idle=True)
        except IOError as e:
            # Stop the worker, everything is shutting down anyway
            self.stop_worker(pid, is_idle=True)
            # Can get EINTR if socket ops are interrupted by signals
            if e.errno != errno.EINTR:
                raise

    def stop_worker(self, pid, is_idle):
        """ Stops the worker with process id pid, deletes shared state and
            stops the thread monitoring the health of this worker

            If worker was idle when it was stopped, the idle worker count is
            also reduced by 1
        """
        if pid not in self.monitors:
            _logger.error("Cannot find worker with pid %s", pid)
            return
        self.send_shutdown(pid)
        self.total_workers -= 1

        if is_idle:
            self.decrement_idle_workers()

        self.monitors[pid].terminate()
        del self.monitors[pid]

        try:
            # Delete process config
            self.config.delete_process_config(pid)
            _logger.info("Deleted process config for worker with pid %s", pid)
        except ConfigNotAvailableException:
            _logger.exception("Network error while deleting config for worker with pid %s", pid)
        except ConfigException:
            _logger.exception("Error deleting config for worker with pid %s", pid)
        except IOError as e:
            # OK to leave a stale config, as its cleaned up by
            # the pool manager on start
            if e.errno != errno.EINTR:
                raise

    def stop_all_workers(self):
        """ Stops all the worker processes and associated monitor threads
            Does not delete the process configs
            Called when the main pool manager process is shutting down
        """
        self.total_workers = 0
        self.reset_idle_workers()

        _logger.info("Shutting down all workers")
        for pid in self.monitors:
            self.send_shutdown(pid)
            self.monitors[pid].terminate()

        sleep(PROCESS_WAIT_TIME)

    def clear_stale_process_configs(self):
        """ Deletes all process configs
            Called every time the pool manager is started

            Returns True on success, False on failure
        """
        success = False
        pids = None
        try:
            pids = self.config.get_all_pids()
            success = True
        except ConfigNotAvailableException:
            _logger.exception("Network error while getting all pids")
        except ConfigException:
            _logger.exception("Error getting all pids")

        if not pids:
            return success

        deleted_count = 0
        for pid in pids:
            try:
                self.config.delete_process_config(pid)
                _logger.info("Deleted stale process config for worker with pid %s", pid)
                deleted_count += 1
            except ConfigNotAvailableException:
                _logger.exception("Network error while deleting stale config for pid %s", pid)
            except ConfigException:
                _logger.exception("Error deleting stale config for pid %s", pid)

        if deleted_count != len(pids):
            _logger.error("%s stale process configs found, could only delete %s", len(pids), deleted_count)
            success = False
        return success

    def start(self):
        _logger.info("Started the smbc service")
        signal.signal(signal.SIGTERM, self.handle_shutdown)

        if not self.clear_stale_process_configs():
            raise InvalidConfigException("Could not fix stale configuration")

        self.start_smbc_server()

        for _ in range(MIN_WORKERS):
            self.start_worker()

        while not self.shutdown.is_set():
            self.shutdown.wait(30)     # Block for 30 seconds or until woken up
            pids = self.monitors.keys()
            for pid in pids:
                if self.shutdown.is_set():
                    return

                if not self.monitors[pid].is_worker_alive():
                    self.stop_worker(pid, is_idle=self.monitors[pid].is_idle())
                    _logger.error("Worker with pid %s exited suddenly, cleaning up....", pid)
                elif self.monitors[pid].should_shutdown():
                    self.stop_worker(pid, is_idle=self.monitors[pid].is_idle())
                    _logger.info("Shutting down worker with pid %s because of too many failures", pid)
                elif self.monitors[pid].should_reset():
                    self.send_reset(pid)
                    _logger.info("Reset worker with pid %s because of inactivity", pid)

            # Replace workers that terminated suddenly
            # This should be done only if total workers < MIN_WORKERS
            if self.total_workers < MIN_WORKERS:
                _logger.warning("Worker count is less than the minimum, starting more workers")
                while self.total_workers < MIN_WORKERS:
                    self.start_worker()

            idle_workers = self.get_idle_workers()
            _logger.info("Idle workers: %s, Total workers: %s", idle_workers, self.total_workers)

            # Keep a buffer of two idle workers
            if idle_workers > 2 and self.total_workers > MIN_WORKERS:
                excess_workers = idle_workers - 2
                stopped_count = 0
                pids = self.monitors.keys()
                for pid in pids:
                    if self.shutdown.is_set():
                        return
                    if self.monitors[pid].is_idle():
                        self.stop_worker(pid, is_idle=True)
                        stopped_count += 1
                        if stopped_count == excess_workers or self.total_workers == MIN_WORKERS:
                            _logger.warning("Too many idle workers, stopped %d of them", stopped_count)
                            # Maintain minimum number of idle and total workers
                            break
            elif idle_workers == 0 and self.total_workers < MAX_WORKERS:
                _logger.warning("All workers are busy, starting a new one to help with the load")
                self.start_worker()

            # Restart the smbc server if it went down
            if not self.is_smbc_server_alive():
                self.start_smbc_server()

            # Reap any zombies
            active_children()


if __name__ == "__main__":
    pmanager = None
    logconfig = "/opt/egnyte/smbc-service/confs/logging.yaml"

    if os.path.exists(logconfig):
        # Set up the logger here
        # Worker processes will inherit this logger setup
        with open(logconfig, 'r') as lconfig:
            config = yaml.load(lconfig.read())
        logging.config.dictConfig(config)

        # Suppress INFO and lower log level messages from urllib3
        logging.getLogger("urllib3").setLevel(logging.WARNING)

        try:
            pmanager = PoolManager()
            pmanager.start()
        except InvalidConfigException:
            _logger.exception("Invalid configuration!")
        except KeyboardInterrupt:
            _logger.exception("Interrupted, exiting...")
        except IOError as e:
            if e.errno != errno.EINTR:
                _logger.exception("Unexpected IOError!")
                raise
        finally:
            if pmanager:
                pmanager.stop_smbc_server()
                pmanager.stop_all_workers()
            _logger.info("Stopped the smbc service")
        logging.shutdown()
        sleep(PROCESS_WAIT_TIME)
