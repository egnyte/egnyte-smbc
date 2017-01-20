""" Etcd is used to store configuration information
    https://coreos.com/etcd/docs/latest/

    This module exposes the ConfigManager class which does the following:
    Sets up the communication channel with the etcd server using the client cert
    Provides an interface to read, update and delete job
    as well as process config information
    Provides an interface to update process and job heartbeat information
    Raises the appropriate exceptions which should be handled by consumers of this module
"""

import os
import time
import etcd

JOBS_ROOT = "/hub/jobs"
NODES_ROOT = "/hub/nodes"
DEFAULT_COMPONENT = "smbc"

PUBLIC_KEY = "/opt/egnyte/node/certs/etcd/client.crt"
PRIVATE_KEY = "/opt/egnyte/node/certs/etcd/client.key"
ROOT_CA_PUBLIC_KEY = "/opt/egnyte/node/certs/etcd/root.crt"

NODE_NAME_FILE = "/node/node-name"

WATCH_TIMEOUT = 120      # Max time to wait for a key to change its value

SECONDS_IN_DAY = 86400

class ConfigException(Exception):
    """ Base class for all configuration related exceptions
        Args:
            message (mandatory) : Descriptive message of exception
            payload (optional)  : Dictionary with additional information
                                  returned by the etcd server for the exception
                                  Keys in this dictionary include:
                                  message (included in main message arg)
                                  cause (included in main message arg)
                                  errorCode (optional)
                                  status (http) (optional)
    """
    def __init__(self, message, payload=None):
        super(ConfigException, self).__init__(message)
        self.payload = payload


class ConfigValueError(ConfigException):
    """ Raised if invalid config params were used """
    pass


class InvalidConfigException(ConfigException):
    """ Raised if the configuration is in an invalid state """
    pass


class ConfigNotFoundException(ConfigException):
    """ Raised if the config key does not exist """
    pass

class ConfigClearedException(ConfigException):
    """ Raised if the state of a key at a specified index has been cleared
        and is no longer available """
    pass

class ConfigNotChangedException(ConfigException):
    """ Raised if the config key did not change i.e. if a watch times out """
    pass


class ConfigNotAvailableException(ConfigException):
    """ Raised if the config is currently not available
        Could be because raised because of connection errors
    """
    pass


class ConfigManager(object):

    def __init__(self, component_name=None):
        """ Initialize relevant paths and set up ssl protected connection to
            the etcd server daemon on localhost. Client and server side
            cert validation is performed.

            Raises InvalidConfigException on error
        """
        component_name = component_name or DEFAULT_COMPONENT
        self.node_name = None
        self.client = None
        try:
            with open(NODE_NAME_FILE, 'r') as node_file:
                self.node_name = node_file.readline().strip()
            self.client = etcd.Client(host=self.node_name, protocol="https", cert=(PUBLIC_KEY, PRIVATE_KEY), ca_cert=ROOT_CA_PUBLIC_KEY)
        except IOError:
            raise InvalidConfigException("Error reading node name")
        except etcd.EtcdException as e:
            raise InvalidConfigException(e.message, e.payload)
        # Only the first section of the node name is needed
        self.node_name = self.node_name.split(".")[0]
        self.jobs = os.path.join(JOBS_ROOT, component_name)
        service_prefix = os.path.join(NODES_ROOT, self.node_name)
        service_suffix = os.path.join("services", component_name)
        self.service = os.path.join(service_prefix, service_suffix)

    def __get_dir(self, dir_key, timeout=None, leaves_only=True):
        """ Reads the dir at dir_key and returns a dict representation of it
            Directory keys are returned if leaves_only is False
            This method only returns the first level keys and values

            Raises ConfigValueError, ConfigNotFoundException,
            ConfigNotAvailableException and ConfigException for generic errors
        """
        directory = {}

        try:
            configs = self.client.read(dir_key, recursive=True, wait=False, timeout=timeout)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            # An unwrapped Value Error is seen only if path does not start with '/'
            # All other ValueErrors are wrapped and raised as an EtcdValueError
            raise ConfigValueError("Path to the key %s does not start with /" % dir_key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

        parent_separator_count = dir_key.count("/")

        for config in configs.get_subtree(leaves_only):
            # Only include the first level keys
            if config.key.count("/") - parent_separator_count == 1:
                # Key should not be the full path, use only the suffix
                # Convert the unicode strings to byte strings
                # All internal components use byte strings
                key = config.key.encode("utf-8").rsplit("/")[-1]
                directory[key] = None
                if config.value:              # Could be None if leaves_only is False
                    directory[key] = config.value.encode("utf-8")

        return directory

    def __get_etcd_index_for_dir(self, dir_key):
        """ Reads the dir at dir_key and returns the current value of X-Etcd-Index
            for it

            Raises ConfigValueError, ConfigNotFoundException,
            ConfigNotAvailableException and ConfigException for generic errors
        """
        try:
            configs = self.client.read(dir_key, recursive=False, wait=False)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            # An unwrapped Value Error is seen only if path does not start with '/'
            # All other ValueErrors are wrapped and raised as an EtcdValueError
            raise ConfigValueError("Path to the key %s does not start with /" % dir_key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

        return configs.etcd_index

    def __watch_dir(self, dir_key, index=0, timeout=0):
        """ Watches the dir at dir_key and returns a dict representation of any changes
            starting from index, along with the modified index

            This method only returns the first level keys and values
            This method blocks until the watched directory is changed

            Raises ConfigValueError, ConfigNotFoundException,
            ConfigNotChangedException, ConfigNotAvailableException and
            ConfigException for generic errors
        """
        directory = {}

        try:
            if index:
                configs = self.client.read(dir_key, recursive=True, wait=True, waitIndex=index, timeout=timeout)
            else:
                configs = self.client.read(dir_key, recursive=True, wait=True, timeout=timeout)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            # An unwrapped Value Error is seen only if path does not start with '/'
            # All other ValueErrors are wrapped and raised as an EtcdValueError
            raise ConfigValueError("Path to the key %s does not start with /" % dir_key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdWatchTimedOut as e:
            raise ConfigNotChangedException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdEventIndexCleared as e:
            raise ConfigClearedException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

        parent_separator_count = dir_key.count("/")

        for config in configs.get_subtree(True):
            # Only include the first level keys
            if config.key.count("/") - parent_separator_count == 1:
                # Key should not be the full path, use only the suffix
                # Convert the unicode strings to byte strings
                # All internal components use byte strings
                key = config.key.encode("utf-8").rsplit("/")[-1]
                directory[key] = None
                if config.value:
                    directory[key] = config.value.encode("utf-8")

        return directory, configs.modifiedIndex

    def __get_key(self, key):
        """ Reads the key and returns its value

            Raises ConfigValueError, ConfigNotFoundException,
            ConfigNotAvailableException and ConfigException for generic errors
        """
        try:
            result = self.client.read(key, recursive=False)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            raise ConfigValueError("Path to the key %s does not start with /" % key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

        if result.dir:
            raise ConfigValueError("Cannot read the value of a key which is a directory")
        return result.value.encode("utf-8")

    def __set_key(self, key, **kwargs):
        """ Updates the key
            kwargs must contain either a value or dir = True
            It may also contain a ttl, prevValue or prevExist
            If this is not a directory, sets the value for key
            If a ttl is provided, sets the time to live for this key too
            If prevValue is provided, the update becomes an atomic compare and swap
            If prevExist is False, only creates the new key otherwise only updates it

            Raises ConfigValueError, InvalidConfigException,
            ConfigNotFoundException, ConfigNotAvailableException and
            ConfigException for generic errors
        """
        value = kwargs.get('value')
        ttl = kwargs.get('ttl')
        dir = kwargs.get('dir', False)
        prev_exist = kwargs.get('prevExist', True)
        prev_value = kwargs.get('prevValue')
        secondary_args = {}
        secondary_args['prevExist'] = prev_exist
        if prev_value:
            secondary_args['prevValue'] = prev_value

        # Must provide a value if key is not directory, otherwise value must not be provided
        if not value and not dir:
            raise ConfigValueError("No value was provided for non directory key %s" % key)
        if value and dir:
            raise ConfigValueError("Value cannot be provided for directory key %s" % key)

        try:
            self.client.write(key, value, ttl, dir, **secondary_args)
        except (etcd.EtcdAlreadyExist, etcd.EtcdNotDir, etcd.EtcdCompareFailed) as e:
            raise InvalidConfigException(e.message, e.payload)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            raise ConfigValueError("Path to the key %s does not start with /" % key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

    def __delete_key(self, key, recursive=False, dir=False):
        """ Deletes the key
            If dir is True and recursive is True, deletes the entire directory

            Raises ConfigValueError, InvalidConfigException,
            ConfigNotFoundException, ConfigNotAvailableException and
            ConfigException for generic errors
        """
        # Recursive must be equal to dir i.e. True if dir is True and False if its False
        if dir != recursive:
            raise ConfigValueError("Recursive param value must match dir param value for deletes")

        try:
            self.client.delete(key, recursive, dir)
        except (etcd.EtcdDirNotEmpty, etcd.EtcdNotDir) as e:
            raise InvalidConfigException(e.message, e.payload)
        except etcd.EtcdValueError as e:
            raise ConfigValueError(e.message, e.payload)
        except ValueError:
            raise ConfigValueError("Path to the key %s does not start with /" % key)
        except etcd.EtcdKeyNotFound as e:
            raise ConfigNotFoundException(e.message, e.payload)
        except etcd.EtcdConnectionFailed as e:
            raise ConfigNotAvailableException(e.message, e.payload)
        except etcd.EtcdException as e:
            raise ConfigException(e.message, e.payload)

    def __get_job_config_key(self, root, key):
        """ Reads the value of the key within the job config at root
            Raises the same exceptions as the __get_key method
        """
        job_config_root = os.path.join(self.jobs, root)
        key = os.path.join(job_config_root, key)
        return self.__get_key(key)

    def __get_process_config_key(self, root, key):
        """ Reads the value of the key within the process config at root
            Raises the same exceptions as the __get_key method
        """
        proc_config_root = os.path.join(self.service, root)
        key = os.path.join(proc_config_root, key)
        return self.__get_key(key)

    def __set_job_config_key(self, root, key=None, **kwargs):
        """ Updates the key within root else updates the root dir
            kwargs must contain either a value or dir = True
            It may also contain a ttl, prevValue or prevExist
            If this is not a directory, sets the value for key
            If a ttl is provided, sets the time to live for this key too
            If prevValue is provided, the update becomes an atomic compare and swap
            If prevExist is False, only creates the new key otherwise only updates it

            Raises the same exceptions as __set_key
        """
        job_config_root = os.path.join(self.jobs, root)
        if not key:
            key = job_config_root
        else:
            key = os.path.join(job_config_root, key)
        self.__set_key(key, **kwargs)

    def __set_process_config_key(self, root, key=None, **kwargs):
        """ Updates the key within root if provided else updates the root dir
            kwargs must contain either a value or dir = True
            It may also contain a ttl and prevExist
            If this is not a directory, sets the value for key
            If a ttl is provided, sets the time to live for this key too
            If prevValue is provided, the update becomes an atomic compare and swap
            If prevExist is False, only creates the new key otherwise only updates it

            Raises the same exceptions as __set_key
        """

        proc_config_root = os.path.join(self.service, root)
        if not key:
            key = proc_config_root
        else:
            key = os.path.join(proc_config_root, key)
        self.__set_key(key, **kwargs)

    def __delete_job_config_key(self, root, key=None, recursive=False, dir=False):
        """ Deletes the key in the job config at root if provided
            Otherwise deletes root
            Raises the same exceptions as __delete_key
        """
        job_config_root = os.path.join(self.jobs, root)
        if not key:
            key = job_config_root
        else:
            key = os.path.join(job_config_root, key)
        self.__delete_key(key, recursive, dir)

    def __delete_process_config_key(self, root, key=None, recursive=False, dir=False):
        """ Deletes the key in the process config at root if provided
            Otherwise deletes root
            Raises the same exceptions as __delete_key
        """
        proc_config_root = os.path.join(self.service, root)
        if not key:
            key = proc_config_root
        else:
            key = os.path.join(proc_config_root, key)
        self.__delete_key(key, recursive, dir)

    def get_source_config(self, source_path):
        """ Returns a dict representation of the config for the source
            at source_path
            Raises the same exceptions as __get_dir
        """
        if not source_path.startswith("/"):
            source_path = "/" + source_path
        return self.__get_dir(source_path)

    def get_job_config(self, jobid):
        """ Returns a dict representation of the config for jobid
            Raises the same exceptions as __get_dir
        """
        job_config_root = os.path.join(self.jobs, "config")
        key = os.path.join(job_config_root, str(jobid))
        return self.__get_dir(key)

    def get_process_config(self, pid):
        """ Returns a dict representation of the config for process pid
            Raises the same exceptions as __get_dir
        """
        proc_config_root = os.path.join(self.service, "async")
        key = os.path.join(proc_config_root, str(pid))
        return self.__get_dir(key)

    def get_all_pids(self):
        """ Returns a dictionary of all pids
            Raises the same exceptions as __get_dir
        """
        pids_root = os.path.join(self.service, "async")
        return self.__get_dir(pids_root, leaves_only=False)

    def set_job_status(self, jobid, status):
        """ Adds jobid to "completed" along with its status
            Raises the same exceptions as __set_job_config_key
        """
        kwargs = {}
        kwargs['value'] = str(status)
        kwargs['prevExist'] = False
        kwargs['ttl'] = SECONDS_IN_DAY
        self.__set_job_config_key("completed", str(jobid), **kwargs)

    def update_job_heartbeat(self, jobid):
        """ Updates the heartbeat key of the job represented by jobid
            Raises the same exceptions as __set_job_config_key
        """
        kwargs = {}
        kwargs['value'] = str(long(time.time()*1000))
        kwargs['prevExist'] = True
        root = os.path.join("config", str(jobid))
        self.__set_job_config_key(root, 'heartbeat', **kwargs)

    def update_process_heartbeat(self, pid):
        """ Updates the heartbeat key of the process represented by pid
            Raises the same exceptions as __set_process_config_key
        """
        kwargs = {}
        kwargs['value'] = str(long(time.time()))
        kwargs['prevExist'] = True
        root = os.path.join("async", str(pid))
        self.__set_process_config_key(root, 'heartbeat', **kwargs)

    def update_process_jobid(self, pid, jobid):
        """ Updates the jobid key of the process represented by pid
            Raises the same exceptions as __set_process_config_key
        """
        kwargs = {}
        kwargs['value'] = jobid
        kwargs['prevExist'] = True
        root = os.path.join("async", str(pid))
        self.__set_process_config_key(root, 'jobid', **kwargs)

    def update_job_output(self, jobid, ofile):
        """ Updates the output key of the job represented
            by jobid
            Raises the same exceptions as __set_job_config_key
        """
        kwargs = {}
        kwargs['value'] = ofile
        kwargs['prevExist'] = True
        root = os.path.join("config", jobid)
        self.__set_job_config_key(root, "output", **kwargs)

    def update_job_started(self, jobid):
        """ Updates the started key of the job represented
            by jobid
            Raises the same exceptions as __set_job_config_key
        """
        kwargs = {}
        kwargs['value'] = str(long(time.time()*1000))
        root = os.path.join("config", jobid)
        self.__set_job_config_key(root, "started", **kwargs)

    def update_job_completed(self, jobid):
        """ Updates the completed key of the job represented
            by jobid
            Raises the same exceptions as __set_job_config_key
        """
        kwargs = {}
        kwargs['value'] = str(long(time.time()*1000))
        root = os.path.join("config", jobid)
        self.__set_job_config_key(root, "completed", **kwargs)

    def increment_job_try_count(self, jobid):
        """ Increments the try count for the job represented by jobid
            Raises the same exceptions as __get_job_config_key and
            __set_job_config_key
        """
        root = os.path.join("config", str(jobid))
        old_count = self.__get_job_config_key(root, 'try_count')
        try:
            old_count = int(old_count)
        except ValueError:
            raise InvalidConfigException("try_count with value %s could not be coerced to an int value" % old_count)
        new_count = old_count + 1
        kwargs = {}
        kwargs['value'] = str(new_count)
        kwargs['prevExist'] = True
        kwargs['prevValue'] = str(old_count)
        self.__set_job_config_key(root, 'try_count', **kwargs)

    def increment_process_fail_count(self, pid):
        """ Increments the successive_fail_count for the process represented by pid
            Raises the same exceptions as __get_process_config_key and
            __set_process_config_key
        """
        root = os.path.join("async", str(pid))
        old_count = self.__get_process_config_key(root, 'successive_fail_count')
        try:
            old_count = int(old_count)
        except ValueError:
            raise InvalidConfigException("successive_fail_count with value %s could not be coerced to an int value" % old_count)
        new_count = old_count + 1
        kwargs = {}
        kwargs['value'] = str(new_count)
        kwargs['prevExist'] = True
        kwargs['prevValue'] = str(old_count)
        self.__set_process_config_key(root, 'successive_fail_count', **kwargs)

    def reset_process_fail_count(self, pid):
        """ Resets the successive_fail_count for the process represented by pid
            Raises the same exceptions as __set_process_config_key
        """
        kwargs = {}
        kwargs['value'] = '0'
        kwargs['prevExist'] = True
        root = os.path.join("async", str(pid))
        self.__set_process_config_key(root, 'successive_fail_count', **kwargs)

    def create_process_config(self, pid):
        """ Creates the entire process config directory for pid
            Raises the same exceptions as __set_process_config_key
        """
        # Make the top level directory
        kwargs = {}
        kwargs['dir'] = True
        kwargs['prevExist'] = False
        root = os.path.join("async", str(pid))
        self.__set_process_config_key(root, None, **kwargs)

        # Make the leaves
        kwargs['value'] = str(long(time.time()))
        kwargs['dir'] = False
        self.__set_process_config_key(root, 'heartbeat', **kwargs)

        kwargs['value'] = '0'
        self.__set_process_config_key(root, 'successive_fail_count', **kwargs)

        kwargs['value'] = ' '
        self.__set_process_config_key(root, 'jobid', **kwargs)

    def watch_for_available_jobs(self):
        """ Watch the queue for available jobs and yields any new jobs
            Yields None on error or in special circumstances which include:
                Etcd server sending an empty response
                Watcher index is cleared because the cache limit is hit
            Retrying watch_for_available_jobs yields the expected result

            Raises the same exceptions as __watch_dir
        """
        jobs_dir = os.path.join(self.jobs, "available")
        local_index = 0
        while True:
            job = None
            try:
                job, index = self.__watch_dir(jobs_dir, local_index, WATCH_TIMEOUT)
            except ConfigClearedException:
                # Lost events, reset to last known event index (X-Etcd-Index)
                # Resume watching at this index + 1
                # https://coreos.com/etcd/docs/latest/api.html#waiting-for-a-change
                index = self.__get_etcd_index_for_dir(jobs_dir)
            local_index = index + 1
            yield job

    def claim_available_job(self, jobid):
        """ Claims jobid from the available queue by deleting it
            to make it unavailable to others
            Raises the same exceptions as __delete_job_config_key
        """
        self.__delete_job_config_key("available", str(jobid))

    def delete_process_config(self, pid):
        """ Deletes the process config directory for pid
            Raises the same exceptions as __delete_process_config_key
        """
        root = os.path.join("async", str(pid))
        self.__delete_process_config_key(root, None, True, True)

    def watch_process_config(self, pid):
        """ Watch the process config directory for pid for any changes
            Blocks until there is a change
            Yields None on error or special circumstances which include:
                Etcd server sending an empty response
                Watcher index is cleared because the cache limit is hit
            Retrying watch_process_config yields the expected result

            Raises the same exceptions as __watch_dir
        """
        proc_config_root = os.path.join(self.service, "async")
        proc_dir = os.path.join(proc_config_root, str(pid))
        local_index = 0
        while True:
            config = None
            try:
                config, index = self.__watch_dir(proc_dir, local_index)
            except ConfigClearedException:
                # Lost events, reset to last known event index (X-Etcd-Index)
                # Resume watching at this index + 1
                # https://coreos.com/etcd/docs/latest/api.html#waiting-for-a-change
                index = self.__get_etcd_index_for_dir(proc_dir)
            local_index = index + 1
            yield config
