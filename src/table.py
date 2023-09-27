import logging
import os
from abc import ABC, abstractmethod
from typing import Optional, cast

from src.binary_tree import AvlBinaryTree
from src.config import DATABASE_FD, DATABASE_FILE_NAME
from src.helper import seek_db_fd
from src.row import IntColumn, Row, SmallInt

logger = logging.getLogger(__name__)


class Table(ABC):
    """
    Our table structure will be like

    Row count(2 bytes) | (Rows)

    ignore the pipe, the data will be contiguously allocated, i.e first 2 byte will be row count
    that effectively limits our table size to 2**16 - 1 = 65,535 rows
    First row will always be the root raw, so whenever a table is initialized we will load the root row first
    by fetching Row.size() bytes from 2nd position in file.

    Row data format is documented in Row class

    """

    SPACE_USED_FOR_SAVING_ROW_COUNT = SmallInt(2)
    SPACE_USED_FOR_ROOT_ROW_ADDRESS = 4
    ROOT_ROW_ADDRESS_OFFSET_IN_FILE = SPACE_USED_FOR_SAVING_ROW_COUNT

    def __init__(self):
        self.root_row: Optional[Row] = None
        self.row_count: SmallInt = SmallInt(0)
        self.binary_tree = AvlBinaryTree(table=self)
        self.cached_rows: dict[IntColumn, Row] = {}
        self.dirty_rows: set[Row] = set()
        self.load()

    def get_row(self, offset) -> Row:
        if offset not in self.cached_rows:
            row = Row.fetch_row(location_in_file=offset, table=self)
            if row:
                self.cached_rows[offset] = row
        else:
            row = self.cached_rows[offset]

        return row

    @abstractmethod
    def load(self):
        """
        We load the initial table metadata like root_row and count here
        """
        ...

    def flush_rows_to_disk(self, rows: set[Row]):
        for row in rows:
            assert row.offset is not None, f"{row=} offset is not set"

        # with open(DATABASE_FILE_NAME, "r+b") as file:
        for row in rows:
            seek_db_fd(row.offset)
            DATABASE_FD.write(row.serialize())
        # Contrary to what one might think that, row_count should be increased by len(rows),
        # its to be noted that right now we only support one insert at a time and rows simply holds
        # row nodes whose pointers are updated due to this new insertion
        self.row_count = SmallInt(self.row_count + 1)
        seek_db_fd(0)

        DATABASE_FD.write(self.row_count.serialize())
        DATABASE_FD.flush()

    def insert(self, row: Row):
        row.offset = self.offset_for_a_new_row

        self.cached_rows[row.offset] = row

        self.binary_tree._row_to_insert = cast(Row, row)
        existing_root_row = self.root_row
        self.root_row = self.binary_tree.insert(row_to_insert=row, root_row=self.root_row)
        if self.root_row != existing_root_row:
            DATABASE_FD.seek(self.SPACE_USED_FOR_SAVING_ROW_COUNT)
            DATABASE_FD.write(self.root_row.offset.serialize())
        logger.debug(f"{self.root_row=} {row=} {row.offset=} {self.dirty_rows=}")

        self.flush_rows_to_disk(rows=self.dirty_rows)
        self.dirty_rows = set()

    def raw_data(self) -> bytes:
        """
        Helper method used in test to get the raw representation of a table in file
        """

        seek_db_fd(0)
        return DATABASE_FD.read()
        # with open(DATABASE_FILE_NAME, "rb") as file:
        #     return file.read()

    @property
    def offset_for_a_new_row(self):
        """
        No matter how we save our table data, linearly or in a binary tree form. The location of a newly
        added row will always be a simple math of row_count * row_size + any_padding or anything.
        It's the row.left_child_offset and row.right_child_offset that matters and are key component
        during traversing the tree.
        """
        return IntColumn(
            self.row_count * Row.size() + self.SPACE_USED_FOR_SAVING_ROW_COUNT + self.SPACE_USED_FOR_ROOT_ROW_ADDRESS
        )


class Student(Table):
    def load(self):
        if not os.path.exists(DATABASE_FILE_NAME):
            # Create the file if it doesn't exist
            with open(DATABASE_FILE_NAME, "wb"):
                pass
        logger.debug(f"Loading table from disk file {DATABASE_FILE_NAME}")
        # with open(DATABASE_FILE_NAME, "rb+") as file:

        seek_db_fd(0)
        row_count_raw = DATABASE_FD.read(self.SPACE_USED_FOR_SAVING_ROW_COUNT)
        if row_count_raw:
            self.row_count = SmallInt.deserialize(raw_byte_data=row_count_raw)
            if self.row_count > 0:
                self.root_row = self.get_row(
                    offset=IntColumn.deserialize(DATABASE_FD.read(self.SPACE_USED_FOR_ROOT_ROW_ADDRESS))
                )

        logger.debug(f"{row_count_raw=} {self.row_count=}")


student_table = Student()
