import logging
import os
import unittest
from random import randint
from time import time

from src.config import DATABASE_FILE_NAME
from src.pager import pager
from src.row import IntColumn, Row, StrColumn
from src.table import Student

logger = logging.getLogger(__name__)


# logging.getLogger("main").setLevel(logging.WARN)
# logging.getLogger("table").setLevel(logging.WARN)
# logging.getLogger("binary_tree").setLevel(logging.WARN)
class BaseTestClass(unittest.TestCase):
    def setUp(self) -> None:
        pager.open()
        self.student_table = Student()
        self.assertEqual(self.student_table.row_count, 0)
        self.assertIsNone(self.student_table.root_row)

    def tearDown(self) -> None:
        pager.close()
        os.remove(DATABASE_FILE_NAME)

class TestBulkInsertion(BaseTestClass):
    def test_insert_lotta_rows(self):
        num_rows = 10000
        t = time()
        logger.info(f"inserting {num_rows} rows")
        row_ids_to_insert = []

        for i in range(num_rows):
            # row_id = randint(1, 1000000)
            row_ids_to_insert.append(i)
            row = Row(id=IntColumn(i), name=StrColumn(f"mir chacha{str(i)}"))
            # logger.info(f"Inserting row number {i} data: {row}")
            self.student_table.insert(row=row)
        row_ids_to_insert.sort()
        self.student_table = Student()
        self.assertEqual(self.student_table.row_count, num_rows)

        # Now we assert the correctness of the tree, by fetching all child and checking whether they are sorted or not
        rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
        row_ids = [row.id.val for row in rows]
        self.assertTrue(all(row_ids[i] <= row_ids[i + 1] for i in range(len(row_ids) - 1)))
        self.assertEqual(row_ids, row_ids_to_insert)
        logger.info(f"Time took {time() - t}")

unittest.main()