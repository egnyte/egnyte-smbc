""" Module for the SmbRead class which encapsulates the download or read
    functionality
"""

import os
import stat
import errno
import logging
import smbc

from smb import Smb
from smb import (SmbPermissionsException, SmbObjectNotFoundException,
                 SmbTimedOutException, SmbConnectionException,
                 SmbNotaFileException, InvalidSmbConfigException)

READ_SIZE = 8192

_logger = logging.getLogger(__name__)


class SmbRead(Smb):
    """ Class for the SMB read operation
        Given a server, share and path to the file, it reads the source file
        and creates a copy of it locally
    """
    def __init__(self, smb_config, config_manager):
        super(SmbRead, self).__init__(smb_config, config_manager)
        self.validate_smb_read_config()
        if "folder" in self.smb_config:
            self.smb_url = self.smb_url + "/" + self.smb_config["folder"]
        self.smb_url = self.smb_url + "/" + self.smb_config["file_path"]
        self.create_output_file(mode="wb")
        _logger.info("Url for smb task is %s", self.smb_url)

    def validate_smb_read_config(self):
        """ Validates config to check that params specific to the read op
            are present
            Raises InvalidSmbConfigException if params are missing
        """
        if "file_path" not in self.smb_config or not self.smb_config["file_path"].strip():
            raise InvalidSmbConfigException("File path was not found in the configuration")
        # Strip any leading "/"
        self.smb_config["file_path"] = self.smb_config["file_path"].lstrip("/")

    def execute(self):
        """ Runs the smb op, raises appropriate exceptions on failure """
        source = None
        try:
            st = self.context.stat(self.smb_url)
            is_folder = stat.S_ISDIR(st[stat.ST_MODE])

            if is_folder:
                # Cannot read a folder, raise an error
                raise SmbNotaFileException("%s is not a file!" % self.smb_url)

            # Download the file
            source = self.context.open(self.smb_url, os.O_RDONLY)
            while True:
                data = source.read(READ_SIZE)
                if not data:
                    break
                self.write_output(data, raw=True)
                self.send_heartbeat()
        except smbc.PermissionError as e:
            raise SmbPermissionsException(str(e))
        except smbc.NoEntryError as e:
            raise SmbObjectNotFoundException(str(e))
        except ValueError as e:
            if e.args[0] == errno.EINVAL:
                raise SmbObjectNotFoundException("%s was not found" % self.smb_url)
            _logger.exception("Exception in smb read")
            raise
        except smbc.TimedOutError as e:
            raise SmbTimedOutException(str(e))
        except smbc.ConnectionRefusedError as e:
            raise SmbConnectionException(str(e))
        finally:
            if source:
                source.close()
