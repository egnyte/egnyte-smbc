""" Module for the SmbList class which encapsulates the directory listing
    + stat functionality.
"""

import stat
import errno
import logging
import smbc

from smb import Smb, MAX_DEPTH
from smb import (SmbPermissionsException, SmbObjectNotFoundException,
                 SmbTimedOutException, SmbConnectionException,
                 SmbDepthExceededException, InvalidSmbConfigException,
                 SERVER_LOGGER)

_logger = logging.getLogger(__name__)


class SmbList(Smb):
    """ Class for the SMB directory listing operation
        Given a server, share and optional folder, lists the contents of the
        directory up to MAX_DEPTH and stats each object in the tree

        If config_manager is None this is a synchronous op i.e. the results are
        served immediately and etcd is not used for config management and
        publication of results
    """
    def __init__(self, smb_config, config_manager=None):
        global _logger
        if not config_manager:
            # Synchronous task, update to the correct logger
            _logger = logging.getLogger(SERVER_LOGGER)
        super(SmbList, self).__init__(smb_config, config_manager)
        self.validate_smb_list_config()
        if "folder" in self.smb_config:
            self.smb_url = self.smb_url + "/" + self.smb_config["folder"]
        self.depth = None
        if "depth" in self.smb_config:
            try:
                self.depth = int(self.smb_config["depth"])
            except ValueError:
                _logger.exception("Provided depth is not an integer!")
                raise InvalidSmbConfigException("%s is not a valid value for the list depth" % self.smb_config["depth"])
        else:
            # Depth must be specified for synchronous list ops
            if not config_manager:
                raise InvalidSmbConfigException("depth not specified for synchronous list!")
        self.create_output_file()
        self.filter_folders = self.smb_config["filter"] == "folder"
        _logger.info("Url for smb task is %s", self.smb_url)

    def validate_smb_list_config(self):
        """ Validates config to check that params specific to the list op
            are present
            Raises InvalidSmbConfigException if params are missing
        """
        if "filter" not in self.smb_config or not self.smb_config["filter"].strip():
            raise InvalidSmbConfigException("filter value was not found in the configuration")
        self.smb_config["filter"] = self.smb_config["filter"].lower()
        if self.smb_config["filter"] != "folder" and self.smb_config["filter"] != "file":
            raise InvalidSmbConfigException("%s is not a valid filter value" % self.smb_config["filter"])

    def execute(self, url=None, parent=None, depth=0):
        """ Runs the smb op, raises appropriate exceptions on failure

            Each call to execute writes a JSON object representing a
            file or folder to the output file. Each JSON object has a parent
            field with the name of its parent folder. Files have an additional
            size field. Both files and folders have an mtime field.

            Only objects that match the filter are listed. E.g. if the filter
            is set to folder, only folders will be listed.

            Some examples of the JSON objects representing files and folders:

            { name: "animals", parent: "", mtime: 3489038410342 }
            { name: "dog.bin", parent: "animal", size: 1024, mtime: 3089038410342 }
        """
        if depth > MAX_DEPTH:
            _logger.critical("Maximum depth of recursion exceeded")
            raise SmbDepthExceededException("Folder structure is too deep")

        if self.depth and depth > self.depth:
            # Don't proceed beyond self.depth
            return

        url = url or self.smb_url
        fname = url.rsplit("/", 1)[1]        # File or folder name
        _logger.debug("File or folder name is %s", fname)

        try:
            st = self.context.stat(url)
            is_folder = stat.S_ISDIR(st[stat.ST_MODE])

            if self.filter_folders == is_folder:
                # Dictionary to hold the data returned by the stat call
                data = {"name" : "", "parent" : "", "mtime" : ""}
                data["mtime"] = str(st[stat.ST_MTIME])
                data["name"] = fname
                if parent:
                    data["parent"] = parent
                if not is_folder:
                    data["size"] = str(st[stat.ST_SIZE])
                _logger.debug("Stat call output is %r", data)
                self.write_output(data)
                del data

            # If this is a folder, list and traverse the subtree too
            if stat.S_ISDIR(st[stat.ST_MODE]):
                children = self.context.opendir(url).getdents()
                for child in children:
                    # Get the child object's name as a byte string
                    child_name = child.name.encode("utf-8")
                    if child_name == "." or child_name == "..":
                        continue
                    _logger.debug("Execute called with %s, %s, %d", url + "/" + child_name, fname, depth+1)
                    self.execute(url + "/" + child_name, fname, depth+1)
            # Heartbeats are only needed for asynchronous ops
            if self.config_manager:
                self.send_heartbeat()
        except smbc.PermissionError as e:
            raise SmbPermissionsException(str(e))
        except smbc.NoEntryError as e:
            raise SmbObjectNotFoundException(str(e))
        except ValueError as e:
            if e.args[0] == errno.EINVAL:
                raise SmbObjectNotFoundException("%s was not found" % url)
            _logger.exception("Exception in smb list")
            raise
        except smbc.TimedOutError as e:
            raise SmbTimedOutException(str(e))
        except smbc.ConnectionRefusedError as e:
            raise SmbConnectionException(str(e))
