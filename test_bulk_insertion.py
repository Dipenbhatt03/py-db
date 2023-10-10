import logging
import random
import unittest
from time import time

import main
from src.config import DATABASE_FD
from src.row import IntColumn, Row, StrColumn
from src.table import Student

logger = logging.getLogger(__name__)

class TestBulkInsertion(unittest.TestCase):
    def setUp(self) -> None:
        DATABASE_FD.truncate(0)
        DATABASE_FD.flush()
        self.student_table = Student()

    # def tearDown(self) -> None:
    #     DATABASE_FD.truncate(0)
    #     DATABASE_FD.flush()

    def test_insert_lotta_rows(self):
        num_rows = 10000
        t = time()
        logger.info(f"inserting {num_rows} rows")
        logger.info(f"Existing data {main.student_table.raw_data()}")
        row_ids_to_insert = []
        for i in range(num_rows):
            # logger.info(f"Inserting {i} record")
            # row_id = random.randint(1, 1000000)
            row_id = i
            row_ids_to_insert.append(row_id)
            self.student_table.insert(
                row=Row(id=IntColumn(row_id), name=StrColumn(f"mir chacha{str(i)}"), table=self.student_table)
            )
        row_ids_to_insert.sort()
        self.assertEqual(self.student_table.row_count, num_rows)

        # Now we assert the correctness of the tree, by fetching all child and checking whether they are sorted or not
        rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
        row_ids = [row.id.val for row in rows]
        self.assertTrue(all(row_ids[i] <= row_ids[i + 1] for i in range(len(row_ids) - 1)))
        self.assertEqual(row_ids, row_ids_to_insert)
        logger.info(f"Time took {time() - t}")


unittest.main()