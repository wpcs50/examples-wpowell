"""sqlite_db:
    Methods for creating and deleting a sqlite3 database for emat.
    A Sqlite3 database is a single file.
    The class knows the set of sql files needed to create the necessary tables
"""

import os
from typing import List
import sqlite3
import atexit
import pandas as pd
import warnings
from typing import AbstractSet
import numpy as np
import uuid
import re

# from .query import sql_queries as sq
# from .database import Database
# from ...util.deduplicate import reindex_duplicates
# from ...exceptions import DatabaseVersionWarning, DatabaseVersionError, DatabaseError, ReadOnlyDatabaseError

# from ...util.loggers import get_module_logger
# _logger = get_module_logger(__name__)
# import logging

# from ...util.docstrings import copydoc

sqlite3.register_adapter(np.int64, int)

def _to_uuid(b):
    """Convert a value to a UUID"""
    if isinstance(b, uuid.UUID):
        return b
    if isinstance(b, bytes):
        if len(b)==16:
            return uuid.UUID(bytes=b)
        else:
            try:
                return uuid.UUID(b.decode('utf8'))
            except:
                return uuid.UUID(bytes=b'\xDE\xAD\xBE\xEF' * 4)
    if pd.isna(b):
        return uuid.UUID(bytes=b'\x00' * 16)
    try:
        return uuid.UUID(b)
    except:
        return uuid.UUID(bytes=b'\xDE\xAD\xBE\xEF' * 4)



class SQLiteDB:
    """
    SQLite implementation of the :class:`Database` abstract base class.

    Args:
        database_path (str, optional): file path and name of database file
            If not given, a database is initialized in-memory.
        initialize (bool or 'skip', default False):
            Whether to initialize emat database file.  The value of this argument
            is ignored if `database_path` is not given (as in-memory databases
            must always be initialized).  If given as 'skip' then no setup
            scripts are run, and it is assumed that all relevant tables already
            exist in the database.
        readonly (bool, default False):
            Whether to open the database connection in readonly mode.
        check_same_thread (bool, default True):
            By default, check_same_thread is True and only the creating thread
            may use the connection. If set False, the returned connection may be
            shared across multiple threads.  The dask distributed evaluator has
            workers that run code in a separate thread from the model class object,
            so setting this to False is necessary to enable SQLite connections on
            the workers.
    """

    def __init__(
            self,
            database_path=":memory:",
            initialize=False,
            readonly=False,
            check_same_thread=False,
            update=True,
                ):
        # super().__init__(readonly=readonly)

        self.database_path = database_path

        if self.database_path == ":memory:":
            initialize = True
        # in order:
        self.modules = {}
        if initialize == 'skip':
            self.conn = self.__create(
                [],
                wipe=False,
                check_same_thread=check_same_thread,
            )
        elif initialize:
            self.conn = self.__create(
                ["create_schema.sql"],
                wipe=True,
                check_same_thread=check_same_thread,
            )
        elif readonly:
            self.conn = sqlite3.connect(
                f'file:{database_path}?mode=ro',
                uri=True,
                check_same_thread=check_same_thread,
            )
        else:
            # self.conn = self.__create(
            #     ["emat_db_init.sql", "meta_model.sql"],
            #     wipe=False,
            #     check_same_thread=check_same_thread,
            # )
            #https://www.sqlite.org/uri.html
            self.conn = sqlite3.connect(
                f'file:{database_path}?mode=rw',
                uri=True,
                check_same_thread=check_same_thread,
            )

        atexit.register(self.conn.close)


    def __create(self, filenames, wipe=False, check_same_thread=None):
        """
        Call sql files to create sqlite database file
        """
       
        # close connection and delete file if exists
        if self.database_path != ":memory:" and wipe:
            self.__delete_database()
        try:
            conn = sqlite3.connect(self.database_path, check_same_thread=check_same_thread)
        except sqlite3.OperationalError as err:
            # raise DatabaseError(f'error on connecting to {self.database_path}') from err
            raise ("DatabaseError")
        for filename in filenames:
            filepath = os.path.join("query",filename)
            self.__apply_sql_script(conn, filepath)
            # self.__apply_sql_script(conn, filename)
        return conn

    def __apply_sql_script(self, connection, filename):
        with connection:
            cur = connection.cursor()
            # _logger.info("running script " + filename)
            contents = (
                self.__read_sql_file(
                    os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        filename
                    )
                )
            )
            for q in contents.split(';'):
                # print (q)
                z = cur.execute(q).fetchall()
                if z:
                    # _logger.error(f"Unexpected output in database script {filename}:\n{q}\n{z}")
                    print (f"Unexpected output in database script {filename}:\n{q}\n{z}")

    def vacuum(self):
        self.conn.cursor().execute('VACUUM')

    def update_database(self, queries, on_error='ignore'):
        """
        Update database for compatability with tmip-emat 0.4
        """
        if self.readonly:
            # raise DatabaseVersionError("cannot open or update an old database in readonly")
            raise print ("cannot open or update an old database in readonly")
        else:
            warnings.warn(
                f"updating database file",
                # category=DatabaseVersionWarning,
            )
        with self.conn:
            cur = self.conn.cursor()
            for u in queries:
                try:
                    cur.execute(u)
                except:
                    if on_error in ('log','raise'):
                        # _logger.error(f"SQL Query:\n{u}\n")
                        print (f"SQL Query:\n{u}\n")
                    if on_error == 'raise':
                        raise


    def __read_sql_file(self, filename):
        """
        helper function to load sql files to create database
        """
        sql_file_path = filename
        # _logger.debug(sql_file_path)
        with open(sql_file_path, 'r') as fil:
            all_lines = fil.read()
        return all_lines
        
    def __delete_database(self):
        """
        Delete the sqlite database file
        """
        if os.path.exists(self.database_path):
            os.remove(self.database_path)

    def _raw_query(self, qry=None, table=None, bindings=None):
        if qry is None and table is not None:
            qry = f"PRAGMA table_info({table});"
        with self.conn:
            cur = self.conn.cursor()
            if isinstance(bindings, pd.DataFrame):
                cur.executemany(qry, (dict(row) for _,row in bindings.iterrows()))
            elif bindings is None:
                cur.execute(qry)
            else:
                cur.execute(qry, bindings)
            try:
                cols = ([i[0] for i in cur.description])
            except:
                df = None
            else:
                df = pd.DataFrame(cur.fetchall(), columns=cols)
        return df

    def get_db_info(self):
        """
        Get a short string describing this Database

        Returns:
            str
        """
        return f"SQLite @ {self.database_path}"

