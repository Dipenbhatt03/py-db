import logging
import os
import struct
from abc import ABC, abstractmethod
from typing import Optional, cast

from config import DATABASE_FILE_NAME
from row import Row

logger = logging.getLogger(__name__)


class BinaryTree:
    """
    A class that implements binary tree insertion and search.
    The way to use this class is, a table instantiates this class as one of its property and during insert,
    calls the insert of this class which returns the same row while setting the row to be inserted parent
    and updating the left right pointers of the parent.

    """

    def __init__(self):
        self.meta_info = {}

    def insert(self, row_to_insert: Row, root_row: Row):
        logger.debug(f"{row_to_insert=} {root_row=}")
        if root_row is None:
            root_row = row_to_insert
        elif row_to_insert.id.val <= root_row.id.val:
            row_to_insert = self.insert(
                row_to_insert=row_to_insert, root_row=Row.fetch_row(root_row.left_child_offset.val)
            )
            root_row.left_child_offset.val = row_to_insert.offset
            row_to_insert.parent = root_row
        else:
            row_to_insert = self.insert(
                row_to_insert=row_to_insert, root_row=Row.fetch_row(root_row.right_child_offset.val)
            )
            root_row.right_child_offset.val = row_to_insert.offset
            row_to_insert.parent = root_row
        return root_row

    def traverse(
        self,
        root_row: Optional[Row],
        depth=0,
        row_id_to_search: Optional[int] = None,
        rows_traversed: Optional[list[Row]] = None,
    ) -> list[Row]:
        """
        We do traversal in a DFS format
        """

        self.meta_info["depth"] = depth
        if rows_traversed is None:
            rows_traversed = []
        if root_row is None:
            return rows_traversed
        if row_id_to_search is None:
            logger.info(f"{root_row=} {row_id_to_search=} {depth=}")

            rows_traversed = self.traverse(
                root_row=Row.fetch_row(root_row.left_child_offset.val),
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )
            rows_traversed.append(root_row)

            rows_traversed = self.traverse(
                root_row=Row.fetch_row(root_row.right_child_offset.val),
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )
        else:
            # if row_id_to_search is not None and root_row.id.val == row_id_to_search or row_id_to_search is None:
            if row_id_to_search <= root_row.id.val:
                # if less than we search in left tree
                if row_id_to_search == root_row.id.val:
                    rows_traversed.append(root_row)
                rows_traversed = self.traverse(
                    root_row=Row.fetch_row(root_row.left_child_offset.val),
                    rows_traversed=rows_traversed,
                    row_id_to_search=row_id_to_search,
                    depth=depth + 1,
                )

            else:
                # if value is greater we search in right tree
                rows_traversed = self.traverse(
                    root_row=Row.fetch_row(root_row.right_child_offset.val),
                    rows_traversed=rows_traversed,
                    row_id_to_search=row_id_to_search,
                    depth=depth + 1,
                )

        return rows_traversed


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

    SPACE_USED_FOR_SAVING_ROW_COUNT = 4

    def __init__(self):
        self.root_row: Optional[Row] = None
        self.row_count: int = 0
        self.offset = 0  # used later to know the offset from where the data of this table exist
        # self.lru_row_cache: dict[int, Row] = {}
        self.binary_tree = BinaryTree()
        self.load()

    @abstractmethod
    def load(self):
        """
        We load the initial table metadata like root_row and count here
        """
        ...

    def flush_rows_to_disk(self, rows: list[Row]):
        for row in rows:
            assert row.offset is not None, f"{row=} offset is not set"
        with open(DATABASE_FILE_NAME, "r+b") as file:
            for row in rows:
                file.seek(cast(int, row.offset))
                file.write(row.serialize())
            # Contrary to what one might think that, row_count should be increased by len(rows),
            # its to be noted that right now we only support one insert at a time and rows simply holds
            # row nodes whose pointers are updated due to this new insertion
            self.row_count += 1
            file.seek(0)
            file.write(struct.pack("i", self.row_count))

    def insert(self, row: Row):
        row.offset = self.offset_for_a_new_row
        logger.debug(f"row {row} {row.offset=}")
        self.binary_tree.insert(row_to_insert=row, root_row=self.root_row)
        if self.root_row is None:
            self.root_row = row
        rows_to_flush = [row]
        if row.parent:
            rows_to_flush.append(row.parent)
        self.flush_rows_to_disk(rows=rows_to_flush)

    def raw_data(self) -> bytes:
        """
        Helper method used in test to get the raw representation of a table in file
        """
        with open(DATABASE_FILE_NAME, "rb") as file:
            return file.read()

    @property
    def offset_for_a_new_row(self):
        """
        No matter how we save our table data, linearly or in a binary tree form. The location of a newly
        added row will always be a simple math of row_count * row_size + any_padding or anything.
        It's the row.left_child_offset and row.right_child_offset that matters and are key component
        during traversing the tree.
        """
        return self.row_count * Row.size() + self.SPACE_USED_FOR_SAVING_ROW_COUNT


class Student(Table):
    def __init__(self):
        super().__init__()

    def load(self):
        if not os.path.exists(DATABASE_FILE_NAME):
            # Create the file if it doesn't exist
            with open(DATABASE_FILE_NAME, "wb"):
                pass
        logger.debug(f"Loading table from disk file {DATABASE_FILE_NAME}")
        with open(DATABASE_FILE_NAME, "rb+") as file:
            row_count_raw = file.read(self.SPACE_USED_FOR_SAVING_ROW_COUNT)

            if row_count_raw:
                self.row_count = struct.unpack("i", row_count_raw)[0]
                if self.row_count > 0:
                    self.root_row = Row.deserialize(file.read(Row.size()))
                    self.root_row.offset = self.SPACE_USED_FOR_SAVING_ROW_COUNT
            logger.debug(f"{row_count_raw=} {self.row_count=}")


student_table = Student()
