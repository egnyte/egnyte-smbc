""" This module has a custom logging handler for multiplexing the logs from
    multiple processes.
    Log messages get pushed to a queue by various processes. A dedicated thread
    picks up the messages from the queue and writes them to disk.
    Buffering is handled by the underlying OS implementation
"""
import sys
import errno
import logging
import traceback
import threading

from multiprocessing import Queue
from logging.handlers import RotatingFileHandler


class MultiPlexedLog(logging.Handler):
    """ Class that synchronizes logging between all the processes using a
        multiprocessing queue
        Processes that are logging put log messages into this queue
        A dedicated thread reads log messages from the queue and
        flushes it to the log file on disk
    """
    def __init__(self, filename, mode, maxBytes, backupCount):
        logging.Handler.__init__(self)

        self._handler = RotatingFileHandler(filename, mode, maxBytes, backupCount)
        self.queue = Queue(-1)
        self.noop = False

        t = threading.Thread(target=self.receive)
        t.daemon = True
        t.start()

    def setFormatter(self, fmt):
        logging.Handler.setFormatter(self, fmt)
        self._handler.setFormatter(fmt)

    def receive(self):
        while True:
            try:
                record = self.queue.get()
                if not record:
                    break
                self._handler.emit(record)
            except (KeyboardInterrupt, SystemExit):
                raise
            except EOFError:
                break
            except:
                traceback.print_exc(file=sys.stderr)

    def send(self, s):
        retry = 2
        while retry:
            try:
                self.queue.put_nowait(s)
                break
            except IOError as e:
                # Remote possibility, but could get EINTR here if pool manager
                # or worker is interrupted by a signal in the middle of the put
                if e.errno != errno.EINTR:
                    raise
                # Depending on where the IO op was interrupted, there is a slim
                # chance that we could put the same log message into the queue
                # twice which is OK. That is better than dropping the message
                # altogether
                retry -= 1

    def _format_record(self, record):
        # "Stringify" exc_info and args
        if record.args:
            record.msg = record.msg % record.args
            record.args = None
        if record.exc_info:
            _ = self.format(record)
            record.exc_info = None
        return record

    def emit(self, record):
        if self.noop:
            return
        try:
            s = self._format_record(record)
            self.send(s)
        except (KeyboardInterrupt, SystemExit):
            raise
        except:
            self.handleError(record)

    def flush(self):
        # Change the emit to a no op, put an empty string in the queue to stop
        # the reading thread
        self.noop = True
        self.queue.put_nowait("")

    def close(self):
        self._handler.close()
        logging.Handler.close(self)

