""" Module for the SmbWrite class which encapsulates the upload or write
    functionality
"""

import os
import errno
import logging
import smbc

from smb import Smb
from smb import (SmbPermissionsException, SmbObjectNotFoundException,
                 SmbTimedOutException, SmbConnectionException,
                 SmbWriteException, InvalidSmbConfigException)

READ_SIZE = 8192

_logger = logging.getLogger(__name__)


class SmbWrite(Smb):
    """ Class for the SMB write operation
        Expects a server, share, source file and optional destination folder
        Reads the source file and writes it to the destination
    """
    def __init__(self, smb_config, config_manager):
        super(SmbWrite, self).__init__(smb_config, config_manager)
        self.validate_smb_write_config()
        if "folder" in self.smb_config:
            self.smb_url = self.smb_url + "/" + self.smb_config["folder"]
        self.smb_url = self.smb_url + "/" + os.path.basename(self.smb_config["file_path"].rstrip("/"))
        _logger.info("Url for smb task is %s", self.smb_url)

    def validate_smb_write_config(self):
        """ Validates config to check that params specific to the write op
            are present
            Raises InvalidSmbConfigException if params are missing
        """
        if "file_path" not in self.smb_config or not self.smb_config["file_path"].strip():
            raise InvalidSmbConfigException("Source file path was not found in the configuration")
        if not os.path.isfile(self.smb_config["file_path"]):
            raise InvalidSmbConfigException("Source file at %s does not exist or is not of type file" % self.smb_config["file_path"])

    def execute(self):
        """ Runs the smb op, raises appropriate exceptions on failure """
        source = None
        dest = None
        try:
            # Upload the file
            source = open(self.smb_config["file_path"], "rb")
            dest = self.context.open(self.smb_url, os.O_CREAT | os.O_TRUNC | os.O_WRONLY)
            while True:
                data = source.read(READ_SIZE)
                if not data:
                    break
                ret = dest.write(data)
                if ret < 0:
                    raise SmbWriteException("Write failed for file %s" % self.smb_url)
                self.send_heartbeat()
        except smbc.PermissionError as e:
            raise SmbPermissionsException(str(e))
        except smbc.NoEntryError as e:
            raise SmbObjectNotFoundException(str(e))
        except ValueError as e:
            if e.args[0] == errno.EINVAL:
                raise SmbObjectNotFoundException("%s was not found" % self.smb_url)
            _logger.exception("Exception in smb write")
            raise
        except smbc.TimedOutError as e:
            raise SmbTimedOutException(str(e))
        except smbc.ConnectionRefusedError as e:
            raise SmbConnectionException(str(e))
        finally:
            if source:
                source.close()
            if dest:
                dest.close()
