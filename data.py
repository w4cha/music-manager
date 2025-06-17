import sqlite3
from sqlite3 import Error
import csv
from typing import Generator, Any

class Database:
    def __init__(self, db_path: str = ""):
        self.db = db_path
        self.connection = self.start_connection()

    @property
    def db(self):
        return self._db

    @db.setter
    def db(self, val):
        if isinstance(val, str):
            self._db = val
        else:
            raise DatabaseError("expected value for database connection is a"
                                f" string but got {type(val)} instead")

    def start_connection(self) -> sqlite3.Connection:
        try:
            return sqlite3.connect(self.db)
        except Error as fail:
            raise DatabaseError(f"the next sqlite3 error was raised: {fail}")

    def write_db(self, values: str | tuple | list | dict, many: bool = False) -> None:
        try:
            if many:
                con_obj = self.connection.cursor()
                con_obj.executemany(*self.check_query(values))
            else:
                con_obj = self.connection.cursor()
                con_obj.execute(*self.check_query(values))
            self.connection.commit()
        except Error as fail:
            raise DatabaseError(f"the next sqlite3 error was raised: {fail}")

    def read_db(self, entries: str | tuple, as_row: bool = False) -> Generator[sqlite3.Row | tuple, None, None]:
        try:
            if as_row:
                self.connection.row_factory = sqlite3.Row
            con_obj = self.connection.cursor()
            con_obj.execute(*self.check_query(entries))
            while True:
                fetch_rows = con_obj.fetchmany(20)
                if not fetch_rows:
                    break
                else:
                    for item in fetch_rows:
                        yield item
        except Error as fail:
            raise DatabaseError(f"the next sqlite3 error was raised: {fail}")

    @staticmethod
    def check_query(sql_query: str | tuple | list | dict) -> tuple:
        match sql_query:
            case [str(query), tuple(arguments) | list(arguments)] | (str(query), tuple(arguments) | list(arguments)) | \
                 {"query": str(query), "args": tuple(arguments)}:
                return query, arguments
            case str(sql_query):
                return sql_query,
            case _:
                raise DatabaseError("invalid structure for executable query expected "
                                    "str, list[str, tuple | list], tuple[str, tuple | list] or "
                                    "dict like {query: str, args: tuple}, "
                                    f"got {sql_query} instead.")

    def cvs_(self, read_val: str, output: str, delimiter: str = ",") -> None:
        try:
            con_obj = self.connection.cursor()
            con_obj.execute(*self.check_query(read_val))
            with open(output, "w") as csv_file:
                csv_write = csv.writer(csv_file, delimiter=delimiter)
                csv_write.writerow([row[0] for row in con_obj.description])
                csv_write.writerows(con_obj)
        except Error as fail:
            raise DatabaseError(f"the next sqlite3 error was raised: {fail}")

    def close(self) -> None:
        self.connection.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.connection.close()


class DatabaseError(Exception):
    def __init__(self, message: str = ""):
        self.error = message

    def __str__(self):
        return self.error
