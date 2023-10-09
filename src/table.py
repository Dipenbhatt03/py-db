import logging
from abc import ABC, abstractmethod
from typing import Optional, cast

from src.binary_tree import AvlBinaryTree
from src.data_types import S4Int, US2Int
from src.pager import Page, pager
from src.row import Row

logger = logging.getLogger(__name__)


class Table(ABC):
    """
    So after implementing pager module the database file structure is like
    Page 0.Page 1.Page 2.Page 3. .........Page 1000

    In this, Page 0 will be reserved for metadata which right now will simply be metadata for our hardcoded
    student table.

    So page 0 looks like.
    (Row count 2 bytes)(Root row offset 4 bytes).....padding.....

    First 2 byte will be row count that effectively limits our table size to 2**16 - 1 = 65,535 rows

    Row data format is documented in Row class

    """

    SPACE_USED_FOR_SAVING_ROW_COUNT = 2
    SPACE_USED_FOR_ROOT_ROW_ADDRESS = 4

    def __init__(self):
        self.root_row: Optional[Row] = None
        self.row_count: US2Int = US2Int(0)
        self.binary_tree = AvlBinaryTree(table=self)
        self.load()

    @abstractmethod
    def load(self):
        """
        We load the initial table metadata like root_row and count here
        """
        ...

    def insert(self, row: Row):
        row.offset = self.offset_for_a_new_row
        self.binary_tree._row_to_insert = cast(Row, row)
        self.root_row = self.binary_tree.insert(row_to_insert=row, root_row=self.root_row)
        pager.pager_flush()
        self.row_count = US2Int(self.row_count + 1)

    @property
    def offset_for_a_new_row(self):
        """
        No matter how we save our table data, linearly or in a binary tree form. The location of a newly
        added row will always be a simple math of Page1.size() +  row_count * row_size.
        It's the row.left_child_offset and row.right_child_offset that matters and are key component
        during traversing the tree.
        """

        return S4Int(Page.PAGE_SIZE + self.row_count * Row.size())


class Student(Table):
    def load(self):
        meta_page = pager.get_page(page_num=US2Int(0))

        row_count_raw = meta_page.data[: self.SPACE_USED_FOR_SAVING_ROW_COUNT]
        if row_count_raw:
            self.row_count = US2Int.deserialize(raw_byte_data=row_count_raw)
            if self.row_count > 0:
                row_offset = US2Int.deserialize(
                    raw_byte_data=meta_page.data[
                        self.SPACE_USED_FOR_SAVING_ROW_COUNT : self.SPACE_USED_FOR_SAVING_ROW_COUNT
                        + self.SPACE_USED_FOR_ROOT_ROW_ADDRESS
                    ]
                ).val
                self.root_row = Row.fetch_row(location_in_file=row_offset)

        logger.debug(f"{row_count_raw=} {self.row_count=}")


student_table = Student()
