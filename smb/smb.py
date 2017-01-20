""" Module which has the Smb base class for smb operations and associated
    exception classes
    Classes which implement specific smb tasks inherit from this class
"""
import os
import json
import logging

from time import time

import smbc

# Max recursion depth for smb ops
MAX_DEPTH = 100

# Root folder for output files
OUTPUT_STORE = "/tmp"

# Wait time between successive heartbeats
HEARTBEAT_WAIT = 90

# Server logger name
SERVER_LOGGER = "server"


_logger = logging.getLogger(__name__)


class SmbException(Exception):
    """ Base class for all smb ops related exceptions
        Args:
            message (mandatory) : Descriptive message of exception
    """
    def __init__(self, message):
        super(SmbException, self).__init__(message)


class InvalidSmbConfigException(SmbException):
    """ Raised for invalid or missing params in the smb config
        E.g. username and/or password is missing
    """
    pass


class SmbDepthExceededException(SmbException):
    """ Raised when the folder depth exceeds the maximum allowed depth """
    pass


class SmbPermissionsException(SmbException):
    """ Raised when insufficient permissions prevent the smb op """
    pass


class SmbObjectNotFoundException(SmbException):
    """ Raised when the object was not found """
    pass


class SmbTimedOutException(SmbException):
    """ Raised when the smb op timed out """
    pass


class SmbNotaDirectoryException(SmbException):
    """ Raised if the expected directory is not a directory """
    pass


class SmbNotaFileException(SmbException):
    """ Raised if the expected file is not a file """
    pass


class SmbConnectionException(SmbException):
    """ Raised if the connection to the cifs server is being refused """
    pass


class SmbWriteException(SmbException):
    """ Raised if the smb write fails """
    pass


class Smb(object):
    """ Base class for all smb operations """

    VALID_JOB_TYPES = ("list", "xattr", "read", "write")

    def __init__(self, smb_config, config_manager):
        global _logger
        self.pid = os.getpid()
        self.outf = None
        self.context = None
        self.smb_config = smb_config
        self.config_manager = config_manager
        self.last_heartbeat = None
        if not config_manager:
            # Synchronous task, update to the correct logger
            _logger = logging.getLogger(SERVER_LOGGER)
        self.validate_smb_config()
        self.smb_url = "smb://" + self.smb_config["host"] + "/" + self.smb_config["share"]
        self.setup_smb_context()

    def validate_smb_config(self):
        """ Validates config to check that required params are present
            type indicates the type of smb op i.e. list, xattr, etc.

            Raises InvalidSmbConfigException if params are missing
        """
        required_params = ["domain", "service_user", "service_password", "host", "share"]
        if self.config_manager:
            # Async op, needs a type and a jobid too
            required_params.append("jobid")
            required_params.append("type")
        for x in required_params:
            if x not in self.smb_config or not self.smb_config[x].strip():
                raise InvalidSmbConfigException("%s not found in the configuration" % x)
        if self.config_manager:
            # Verify valid type for async op
            self.smb_config["type"] = self.smb_config["type"].lower()
            if self.smb_config["type"] not in Smb.VALID_JOB_TYPES:
                raise InvalidSmbConfigException("%s is not a valid type value" % self.smb_config["type"])

    def setup_smb_context(self):
        """ Initializes the smb context
            Raises InvalidSmbConfigException on error
        """
        try:
            self.context = smbc.Context()
            self.context.optionNoAutoAnonymousLogin = True
            self.context.optionUseKerberos = True
            self.context.optionFallbackAfterKerberos = True
            # Populate the authentication data that is used for each smb op
            # Server and share information is explicitly provided with each op
            # Domain, username, password information comes from the auth data
            cb = lambda se, sh, w, u, p: (self.smb_config["domain"], self.smb_config["service_user"], self.smb_config["service_password"])
            self.context.functionAuthData = cb
        except (TypeError, RuntimeError) as e:
            _logger.exception("Error initializing the smb context")
            if self.context:
                self.close()
            raise InvalidSmbConfigException(str(e))

    def create_output_file(self, mode="w"):
        """ Create output file for this smb op and cache the file object
            Uses the mode param to decide the create mode, defaults to "w"
            If the file already exists, this reduces to a noop
        """
        if not self.outf:
            if self.config_manager:
                output_file_name = OUTPUT_STORE + self.smb_config["jobid"] + "_" + self.smb_config["type"] + "_" + str(long(time()))
            else:
                output_file_name = OUTPUT_STORE + "sync_" + str(long(time()))
            self.outf = open(output_file_name, mode)
            _logger.info("Created output file %s", output_file_name)

    def write_output(self, data, raw=False):
        """ If data is raw bytes, writes it to disk without any modifications
            Otherwise, dumps the data dict into json format and writes
            it to disk as a byte string
        """
        if data:
            if raw:
                self.outf.write(data)
            else:
                self.outf.write(json.dumps(data, ensure_ascii=False))
                self.outf.write("\n")

    def get_output_file_name(self):
        """ Returns the name of the output file, None on error """
        name = None
        if self.outf:
            name = self.outf.name
        return name

    def send_heartbeat(self):
        """ Post job and process hearbeat information using the
            configuration manager
            Throttled to once every HEARTBEAT_WAIT seconds
        """
        send = True
        now = long(time())
        if self.last_heartbeat and (now - self.last_heartbeat) < HEARTBEAT_WAIT:
            send = False
        if send:
            self.config_manager.update_process_heartbeat(self.pid)
            self.config_manager.update_job_heartbeat(self.smb_config["jobid"])
            self.last_heartbeat = now
            _logger.debug("Sent heartbeat at %s", now)

    def close(self):
        """ Called by workers when the smb op completes or terminates
            abruptly """
        _logger.debug("Smb task was closed")
        if self.outf:
            self.outf.close()
        del self.context
