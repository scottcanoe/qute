#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# This code is distributed under the terms and conditions
# from the Apache License, Version 2.0
#
# http://opensource.org/licenses/apache2.0.php
#
# This code was inspired by:
#  * http://code.activestate.com/recipes/576638-draft-for-an-sqlite3-based-dbm/
#  * http://code.activestate.com/recipes/526618/

"""
A lightweight wrapper around Python's sqlite3 database, with a dict-like interface
and multi-thread access support::

>>> mydict = SqliteDict('some.db', autocommit=True) # the mapping will be persisted to file `some.db`
>>> mydict['some_key'] = any_picklable_object
>>> print mydict['some_key']
>>> print len(mydict) # etc... all dict functions work

Pickle is used internally to serialize the values. Keys are strings.

If you don't use autocommit (default is no autocommit for performance), then
don't forget to call `mydict.commit()` when done with a transaction.

"""

from collections import UserDict
import io
import logging
from numbers import Number
import os
from pathlib import Path
from pickle import dumps, loads, HIGHEST_PROTOCOL as PICKLE_PROTOCOL
from queue import Queue
import sqlite3
import sys
import tempfile
from threading import Thread
import time
import traceback
from typing import (
    Any,
    Callable,
    List,
    Mapping,
    Optional,
    Union,
)


PathLike = Union[str, Path]
logger = logging.getLogger(__name__)




def open(*args, **kw) -> "SqliteDict":
    """See documentation of the SqliteDict class."""
    return SqliteDict(*args, **kw)


def encode(obj: Any) -> sqlite3.Binary:
    """Serialize an object using pickle to a binary format accepted by SQLite."""
    return sqlite3.Binary(dumps(obj, protocol=PICKLE_PROTOCOL))


def decode(obj: sqlite3.Binary) -> Any:
    """Deserialize objects retrieved from SQLite."""
    return loads(bytes(obj))


def reraise(tp, value, tb=None):
    if value is None:
        value = tp()
    if value.__traceback__ is not tb:
        raise value.with_traceback(tb)
    raise value


class SqliteDict(UserDict):

    _VALID_MODES = ("r", "w", "a", "n")

    def __init__(
        self,
        path: Optional[PathLike] = None,
        table: str = "unnamed",
        mode: str = "c",
        autocommit: bool = False,
        journal_mode: str = "DELETE",
        encode: Callable[[Any], sqlite3.Binary] = encode,
        decode: Callable[[sqlite3.Binary], Any] = decode,
        timeout: Number = 5,
    ):
        
        """
        Initialize a thread-safe sqlite-backed dictionary. The dictionary will
        be a table `table` in database file `path`. A single file (=database)
        may contain multiple tables.

        If no `path` is given, a random file in temp will be used (and deleted
        from temp once the dict is closed/deleted).

        If you enable `autocommit`, changes will be committed after each operation
        (more inefficient but safer). Otherwise, changes are committed on `self.commit()`,
        `self.clear()` and `self.close()`.

        Set `journal_mode` to 'OFF' if you're experiencing sqlite I/O problems
        or if you need performance and don't care about crash-consistency.

        The `mode` parameter. Exactly one of:
          'r': open as read-only
          'w': open for read/write, but first clears the target table if it exists.
          'a': open for read/write, but does not clear the target  table if it exists.
          'n': open for read/write, but deletes dabase file entirely before recreating it.

        The `encode` and `decode` parameters are used to customize how the values
        are serialized and deserialized.
        The `encode` parameter must be a function that takes a single Python
        object and returns a serialized representation.
        The `decode` function must be a function that takes the serialized
        representation produced by `encode` and returns a deserialized Python
        object.
        The default is to use pickle.

        The `timeout` defines the maximum time (in seconds) to wait for initial Thread startup.

        """
        
        # validate file mode.
        if mode not in SqliteDict._VALID_MODES:
            raise RuntimeError(f'Unrecognized mode: "{mode}"')
               
        # validate path, defaulting to a temporary file of no path given.
        if path:
            in_temp = False
            path = os.fspath(path)
        else:
            in_temp = True
            fd, path = tempfile.mkstemp(prefix="sqldict")
            os.close(fd)
        path = Path(path)
        
        # check parent directory exists.
        if path.parent and not path.parent.exists():
            raise RuntimeError(f'parent directory "{path.parent}" does not exist')

        self._path = path
        self._mode = mode
        self._in_temp = in_temp
        self._table = table.replace('"', '""') # SQL double-quote escaping
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.encode = encode
        self.decode = decode
        self.timeout = timeout

        
        # mode 'r'
        if mode == "r":
            if not path.exists():
                raise io.UnsupportedOperation(f"path  {path} not writable")
            if table not in SqliteDict.get_tables(path):
                raise io.UnsupportedOperation(f"table  {table} not writable")
            self.conn = self.connect()
            
        # modes 'w'/'a'/'n'
        else:
            self.conn = self.connect()
            self.clear()
        
        elif mode == "a":
            self.conn =  self.connect()
            if table not in self.get_tables(self.path):
                

        # 'a': open for read/write, but does not clear the target  table if it exists.
        elif mode == "a":
            

        
        
        elif mode == "w":
            
        if mode == "n":
            if os.path.exists(path):
                os.remove(path)    
        logger.info(f'opening Sqlite table "{table}" in {path}')
        self.conn = self.connect()

        # mode 'n': create a new database from scratch.
        
        # - delete existing table
        
    
    def get_table(self, table: str, clear: bool = False) -> None:
        
        s = f'CREATE TABLE IF NOT EXISTS "{table}"' + \
             '(key TEXT PRIMARY KEY, value BLOB)'
        self.conn.execute(s)
        self.conn.commit()

            
        if mode == "w":
            self.clear()

    
    #---------------------------------------------------------------------------
    # properties
    
    @property
    def path(self) -> str:
        return self._path
    
    @property
    def mode(self) -> str:
        return self._mode

    @property
    def in_temp(self) -> bool:
        return self._in_temp
    
    @property
    def table(self) -> str:
        return self._table
    
    

    #---------------------------------------------------------------------------
    # dict interface
    


    def clear(self) -> None:
        """
        Clear the dict, and empty its underlying storage.
        """
        if self.mode == "r":
            raise RuntimeError("Refusing to clear read-only SqliteDict")

        # avoid VACUUM, as it gives "OperationalError: database schema has changed"
        CLEAR_ALL = f'DELETE FROM "{self.table}";'
        self.conn.commit()
        self.conn.execute(CLEAR_ALL)
        self.conn.commit()
        
        
    def update(self, other: Mapping) -> None:
        """
        """
        if self.mode == "r":
            raise RuntimeError("")

        try:
            items = items.items()
        except AttributeError:
            pass
        items = [(k, self.encode(v)) for k, v in other.items()]

        UPDATE_ITEMS = f'REPLACE INTO "{self.table}" (key, value) VALUES (?, ?)'
        self.conn.executemany(UPDATE_ITEMS, items)
        if self.autocommit:
            self.commit()
            
            
    def keys(self):
        GET_KEYS = f'SELECT key FROM "{self.table}" ORDER BY rowid'
        for key in self.conn.select(GET_KEYS):
            yield key[0]

    def values(self):
        GET_VALUES = f'SELECT value FROM "{self.table}" ORDER BY rowid'
        for value in self.conn.select(GET_VALUES):
            yield self.decode(value[0])

    def items(self):
        GET_ITEMS = f'SELECT key, value FROM "{self.table}" ORDER BY rowid'
        for key, value in self.conn.select(GET_ITEMS):
            yield key, self.decode(value)


    def __enter__(self) -> "SqliteDict":
        if not hasattr(self, "conn") or self.conn is None:
            self.conn = self.connect()
        return self


    def __iter__(self):
        return self.keys()
    
    
    def __exit__(self, *exc_info) -> None:
        self.close()

    def __str__(self) -> str:
        return f"SqliteDict({self.path})"

    def __repr__(self) -> str:
        return str(self)  # no need of something complex

    def __len__(self) -> int:
        # `select count (*)` is super slow in sqlite (does a linear scan!!)
        # As a result, len() is very slow too once the table size grows beyond trivial.
        # We could keep the total count of rows ourselves, by means of triggers,
        # but that seems too complicated and would slow down normal operation
        # (insert/delete etc).
        GET_LEN = f'SELECT COUNT(*) FROM "{self.table}"'
        rows = self.conn.select_one(GET_LEN)[0]
        return rows if rows is not None else 0

    def __bool__(self):
        # No elements is False, otherwise True
        GET_MAX = f'SELECT MAX(ROWID) FROM "{self.table}"'
        m = self.conn.select_one(GET_MAX)[0]
        # Explicit better than implicit and bla bla
        return True if m is not None else False

    def __contains__(self, key: str) -> bool:
        HAS_ITEM = f'SELECT 1 FROM "{self.table}" WHERE key = ?'
        return self.conn.select_one(HAS_ITEM, (key,)) is not None

    def __getitem__(self, key: str) -> Any:
        GET_ITEM = f'SELECT value FROM "{self.table}" WHERE key = ?'
        item = self.conn.select_one(GET_ITEM, (key,))
        if item is None:
            raise KeyError(key)
        return self.decode(item[0])

    def __setitem__(self, key: str, value: Any) -> None:
        if self.mode == "r":
            raise RuntimeError("Refusing to write to read-only SqliteDict")

        ADD_ITEM = f'REPLACE INTO "{self.table}" (key, value) VALUES (?,?)'
        self.conn.execute(ADD_ITEM, (key, self.encode(value)))
        if self.autocommit:
            self.commit()

    def __delitem__(self, key: str) -> None:
        if self.mode == "r":
            raise RuntimeError("Refusing to delete from read-only SqliteDict")

        if key not in self:
            raise KeyError(key)
        DEL_ITEM = f'DELETE FROM ""{self.table}" WHERE key = ?'
        self.conn.execute(DEL_ITEM, (key,))
        if self.autocommit:
            self.commit()


    #---------------------------------------------------------------------------
    # persistance
            
    
    def connect(self) -> "SqliteMultithread":
        logger.info(f'opening table "{self.table}" in file {self.path}')
        return SqliteMultithread(
            self.path,
            autocommit=self.autocommit,
            journal_mode=self.journal_mode,
            timeout=self.timeout,
        )    
    
    

            
            
    def commit(self, blocking=True):
        """
        Persist all data to disk.

        When `blocking` is False, the commit command is queued, but the data is
        not guaranteed persisted (default implication when autocommit=True).
        """
        if self.conn is not None:
            self.conn.commit(blocking)


    
    def close(self, log: bool = True, force: bool = False) -> None:
        
        if log:
            logger.debug(f"closing {self}")
        
        if hasattr(self, "conn") and self.conn is not None:
            if self.conn.autocommit and not force:
                # typically calls to commit are non-blocking when autocommit is
                # used.  However, we need to block on close() to ensure any
                # awaiting exceptions are handled and that all data is
                # persisted to disk before returning.
                self.conn.commit(blocking=True)
            self.conn.close(force=force)
            self.conn = None
        
        if self.in_temp:
            try:
                os.remove(self.path)
            except:
                pass
            
            
    def terminate(self) -> None:
        """Delete the underlying database file. Use with care."""
        if self.mode == "r":
            raise RuntimeError("Refusing to terminate read-only SqliteDict")

        self.close()

        if self.path == ":memory:":
            return

        logger.info(f"deleting {self.path}")
        try:
            if os.path.isfile(self.path):
                os.remove(self.path)
        except (OSError, IOError):
            logger.exception(f"failed to delete {self.path}")


    

    
    
    def __del__(self):
        # like close(), but assume globals are gone by now (do not log!)
        try:
            self.close(do_log=False, force=True)
        except Exception:
            # prevent error log flood in case of multiple SqliteDicts
            # closed after connection lost (exceptions are always ignored
            # in __del__ method.
            pass
        
        
    #---------------------------------------------------------------------------
    # etc.
    
    
    @staticmethod
    def get_tables(path: PathLike) -> List[str]:
        """get the names of the tables in an sqlite db as a list"""
        path = os.fspath(path)
        if not os.path.isfile(path):
            raise IOError(f"file {path} does not exist")
        GET_tableS = 'SELECT name FROM sqlite_master WHERE type="table"'
        with sqlite3.connect(path) as conn:
            cursor = conn.execute(GET_tableS)
            res = cursor.fetchall()

        return [name[0] for name in res]
    
    

    




class Connection(Thread):
    
    """
    Wrap sqlite connection in a way that allows concurrent requests from
    multiple threads. This is done by internally queueing the requests and 
    processing them sequentially in a separate thread (in the same order they
    arrived).

    """

    def __init__(
        self,
        path: Optional[PathLike] = None,
        autocommit: bool,
        journal_mode: str,
        timeout: Number,
        ):
        super().__init__()
        
        self.path = Path(path)
        self.autocommit = autocommit
        self.journal_mode = journal_mode
        self.timeout = timeout
        
        # use request queue of unlimited size
        self.reqs = Queue()
        self.setDaemon(True)  # python2.5-compatible
        self.exception = None
        self._thread_initialized = None
        
        self.logger = logging.getLogger("sqlitedict.Connection")
        self.start()

    
    def run(self):
        try:
            if self.autocommit:
                conn = sqlite3.connect(
                    self.path, isolation_level=None, check_same_thread=False
                )
            else:
                conn = sqlite3.connect(self.path, check_same_thread=False)
        except Exception:
            self.logger.exception(
                f"Failed to initialize connection for path: {self.path}"
            )
            self.exception = sys.exc_info()
            raise

        try:
            conn.execute(f"PRAGMA journal_mode = {self.journal_mode}")
            conn.text_factory = str
            cursor = conn.cursor()
            conn.commit()
            cursor.execute("PRAGMA synchronous=OFF")
        except Exception:
            self.log.exception("Failed to execute PRAGMA statements.")
            self.exception = sys.exc_info()
            raise

        self._sqlitedict_thread_initialized = True

        res = None
        while True:
            req, arg, res, outer_stack = self.reqs.get()
            if req == "--close--":
                assert res, ("--close-- without return queue", res)
                break
            elif req == "--commit--":
                conn.commit()
                if res:
                    res.put("--no more--")
            else:
                try:
                    cursor.execute(req, arg)
                except Exception:
                    self.exception = (e_type, e_value, e_tb) = sys.exc_info()
                    inner_stack = traceback.extract_stack()

                    # An exception occurred in our thread, but we may not
                    # immediately able to throw it in our calling thread, if it has
                    # no return `res` queue: log as level ERROR both the inner and
                    # outer exception immediately.
                    #
                    # Any iteration of res.get() or any next call will detect the
                    # inner exception and re-raise it in the calling Thread; though
                    # it may be confusing to see an exception for an unrelated
                    # statement, an ERROR log statement from the 'sqlitedict.*'
                    # namespace contains the original outer stack location.
                    self.log.error("Inner exception:")
                    for item in traceback.format_list(inner_stack):
                        self.log.error(item)
                    self.log.error("")  # deliniate traceback & exception w/blank line
                    for item in traceback.format_exception_only(e_type, e_value):
                        self.log.error(item)

                    self.log.error("")  # exception & outer stack w/blank line
                    self.log.error("Outer stack:")
                    for item in traceback.format_list(outer_stack):
                        self.log.error(item)
                    self.log.error("Exception will be re-raised at next call.")

                if res:
                    for rec in cursor:
                        res.put(rec)
                    res.put("--no more--")

                if self.autocommit:
                    conn.commit()

        self.log.debug(f"received: {req}, send: --no more--")
        conn.close()
        res.put("--no more--")

    def check_raise_error(self):
        """
        Check for and raise exception for any previous sqlite query.

        For the `execute*` family of method calls, such calls are non-blocking and any
        exception raised in the thread cannot be handled by the calling Thread (usually
        MainThread).  This method is called on `close`, and prior to any subsequent
        calls to the `execute*` methods to check for and raise an exception in a
        previous call to the MainThread.
        """
        if self.exception:
            e_type, e_value, e_tb = self.exception

            # clear self.exception, if the caller decides to handle such
            # exception, we should not repeatedly re-raise it.
            self.exception = None

            self.log.error(
                "An exception occurred from a previous statement, view "
                'the logging namespace "sqlitedict" for outer stack.'
            )

            # The third argument to raise is the traceback object, and it is
            # substituted instead of the current location as the place where
            # the exception occurred, this is so that when using debuggers such
            # as `pdb', or simply evaluating the naturally raised traceback, we
            # retain the original (inner) location of where the exception
            # occurred.
            reraise(e_type, e_value, e_tb)

    def execute(self, req, arg=None, res=None):
        """
        `execute` calls are non-blocking: just queue up the request and return immediately.
        """
        self._wait_for_initialization()
        self.check_raise_error()

        # NOTE: This might be a lot of information to pump into an input
        # queue, affecting performance.  I've also seen earlier versions of
        # jython take a severe performance impact for throwing exceptions
        # so often.
        stack = traceback.extract_stack()[:-1]
        self.reqs.put((req, arg or tuple(), res, stack))

    def executemany(self, req, items):
        for item in items:
            self.execute(req, item)
        self.check_raise_error()

    def select(self, req, arg=None):
        """
        Unlike sqlite's native select, this select doesn't handle iteration efficiently.

        The result of `select` starts filling up with values as soon as the
        request is dequeued, and although you can iterate over the result normally
        (`for res in self.select(): ...`), the entire result will be in memory.
        """
        res = Queue()  # results of the select will appear as items in this queue
        self.execute(req, arg, res)
        while True:
            rec = res.get()
            self.check_raise_error()
            if rec == "--no more--":
                break
            yield rec

    def select_one(self, req, arg=None):
        """Return only the first row of the SELECT, or None if there are no matching rows."""
        try:
            return next(iter(self.select(req, arg)))
        except StopIteration:
            return None

    def commit(self, blocking=True):
        if blocking:
            # by default, we await completion of commit() unless
            # blocking=False.  This ensures any available exceptions for any
            # previous statement are thrown before returning, and that the
            # data has actually persisted to disk!
            self.select_one("--commit--")
        else:
            # otherwise, we fire and forget as usual.
            self.execute("--commit--")

    def close(self, force=False):
        if force:
            # If a SqliteDict is being killed or garbage-collected, then select_one()
            # could hang forever because run() might already have exited and therefore
            # can't process the request. Instead, push the close command to the requests
            # queue directly. If run() is still alive, it will exit gracefully. If not,
            # then there's nothing we can do anyway.
            self.reqs.put(("--close--", None, Queue(), None))
        else:
            # we abuse 'select' to "iter" over a "--close--" statement so that we
            # can confirm the completion of close before joining the thread and
            # returning (by semaphore '--no more--'
            self.select_one("--close--")
            self.join()

    def _wait_for_initialization(self):
        """
        Polls the 'initialized' flag to be set by the started Thread in run().
        """
        # A race condition may occur without waiting for initialization:
        # __init__() finishes with the start() call, but the Thread needs some time to actually start working.
        # If opening the database file fails in run(), an exception will occur and self.exception will be set.
        # But if we run check_raise_error() before run() had a chance to set self.exception, it will report
        # a false negative: An exception occured and the thread terminates but self.exception is unset.
        # This leads to a deadlock while waiting for the results of execute().
        # By waiting for the Thread to set the initialized mode, we can ensure the thread has successfully
        # opened the file - and possibly set self.exception to be detected by check_raise_error().

        start_time = time.time()
        while time.time() - start_time < self.timeout:
            if self._sqlitedict_thread_initialized or self.exception:
                return
            time.sleep(0.1)
        raise TimeoutError(
            "SqliteMultithread failed to flag initialization within "
            "{:0.0f} seconds.".format(self.timeout)
        )


d = SqliteDict("~/mydb")
