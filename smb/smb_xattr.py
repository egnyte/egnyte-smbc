""" Module for the SmbXattr class which encapsulates the list
    + extended attributes functionality
"""
import errno
import logging
import smbc

from smbc import XATTR_ALL

from smb import Smb, MAX_DEPTH
from smb import (SmbPermissionsException, SmbObjectNotFoundException,
                 SmbDepthExceededException, SmbTimedOutException,
                 SmbConnectionException)

_logger = logging.getLogger(__name__)


class SmbXAttr(Smb):
    """ Class for the SMB extended attributes operation
        Given a server, share and optional folder, lists the contents of the
        directory up to MAX_DEPTH and gets extended attributes for it
    """
    def __init__(self, smb_config, config_manager):
        super(SmbXAttr, self).__init__(smb_config, config_manager)
        if "folder" in self.smb_config:
            self.smb_url = self.smb_url + "/" + self.smb_config["folder"]
        self.create_output_file()
        _logger.info("Url for smb task is %s", self.smb_url)

    def execute(self, url=None, parent=None, parent_sec_desc=None, depth=0):
        """ Runs the smb op, raises appropriate exceptions on failure

            Each call to execute writes a JSON object representing a
            file or folder to the output file. Each JSON object has a parent
            field with the name of its parent folder. Both files and folders
            have a sec_desc (security descriptor) field

            NOTE: JSON object is written only if current security descriptor
            string is different from it's parent security descriptor

            Some examples of the JSON objects are:

            { name: "animals", parent: "",
              sec_desc: "REVISION:1,OWNER:S-1-5-21-1939562231-2713737754-3420863263-1001,
                         GROUP:S-1-5-21-1939562231-2713737754-3420863263-513,
                         ACL:S-1-5-21-1939562231-2713737754-3420863263-1001:0/19/0x001f01ff,
                         ACL:S-1-5-21-2178527575-1394550295-3574831531-1123:0/19/0x001f01ff,
                         ACL:S-1-5-18:0/19/0x001f01ff,ACL:S-1-5-32-544:0/19/0x001f01ff,
                         ACL:S-1-5-32-545:0/19/0x001200a9,ACL:S-1-5-32-545:0/18/0x00000004,
                         ACL:S-1-5-32-545:0/18/0x00000002,ACL:S-1-3-0:0/27/0x10000000"
            }
            { name: "dog.txt", parent: "animals",
              sec_desc: "REVISION:1,OWNER:S-1-5-21-1939562231-2713737754-3420863263-1001,
                         GROUP:S-1-5-21-1939562231-2713737754-3420863263-604,
                         ACL:S-1-5-21-1939562231-2713737754-3420863263-1001:0/19/0x001f01ff,
                         ACL:S-1-5-21-2178527575-1394550295-3574831531-1123:0/19/0x001f01ff,
                         ACL:S-1-5-18:0/19/0x001f01ff,ACL:S-1-5-32-544:0/19/0x001f01ff,
                         ACL:S-1-5-32-545:0/19/0x001200a9,ACL:S-1-5-32-545:0/18/0x00000004,
                         ACL:S-1-5-32-545:0/18/0x00000002,ACL:S-1-3-0:0/27/0x10000000"
            }
        """
        if depth > MAX_DEPTH:
            _logger.critical("Maximum depth of recursion exceeded")
            raise SmbDepthExceededException("Folder structure is too deep")

        url = url or self.smb_url
        fname = url.rsplit("/", 1)[1]        # File or folder name
        _logger.debug("File or folder name is %s", fname)

        try:
            # Get the security descriptor as a byte string
            sec_desc = self.context.getxattr(url, XATTR_ALL).encode("utf-8")
            if not parent_sec_desc or parent_sec_desc != sec_desc:
                # Dictionary to hold the data returned by the xattr call
                data = {"name" : fname, "parent" : "", "sec_desc" : ""}
                data["sec_desc"] = sec_desc
                if parent:
                    data["parent"] = parent
                _logger.debug("Xattr call output is %r", data)
                self.write_output(data)
                del data

            try:
                # If this is a folder, list and get extended attributes for the subtree too
                children = self.context.opendir(url).getdents()
                for child in children:
                    # Get the child object's name as a byte string
                    child_name = child.name.encode("utf-8")
                    if child_name == "." or child_name == "..":
                        continue
                    _logger.debug("Execute called with %s, %s, %d", url + "/" + child_name, fname, depth+1)
                    self.execute(url + "/" + child_name, fname, sec_desc, depth+1)
            except smbc.NotDirectoryError:
                pass
            self.send_heartbeat()
        except smbc.PermissionError as e:
            raise SmbPermissionsException(str(e))
        except smbc.NoEntryError as e:
            raise SmbObjectNotFoundException(str(e))
        except ValueError as e:
            if e.args[0] == errno.EINVAL:
                raise SmbObjectNotFoundException("%s was not found" % url)
            _logger.exception("Exception in smb xattr")
            raise
        except smbc.TimedOutError as e:
            raise SmbTimedOutException(str(e))
        except smbc.ConnectionRefusedError as e:
            raise SmbConnectionException(str(e))
