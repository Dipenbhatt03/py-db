import logging
import random
import struct
import unittest
from time import time

import config  # noqa
import main
from config import DATABASE_FILE_NAME
from main import PrepareStatementResult
from row import IntColumn, Row, StrColumn
from table import Student

logger = logging.getLogger(__name__)
logging.getLogger("main").setLevel(logging.WARN)
logging.getLogger("table").setLevel(logging.WARN)


class TestRowClass(unittest.TestCase):
    def test_serialize_deserialize(self):
        row_v1 = Row(id=IntColumn(2378), name=StrColumn("dipen"))
        row_v1.left_child_offset = IntColumn(2378)
        row_v1.right_child_offset = IntColumn(415)
        row_v2 = Row.deserialize(row_v1.serialize())
        self.assertEqual(row_v2.id.val, row_v1.id.val)
        self.assertEqual(row_v2.name.val, row_v1.name.val)
        self.assertEqual(row_v2.left_child_offset.val, row_v1.left_child_offset.val)
        self.assertEqual(row_v2.right_child_offset.val, row_v1.right_child_offset.val)
        self.assertEqual(row_v1.serialize(), row_v2.serialize())

    def test_invalid_data_is_not_accepted(self):
        with self.assertRaises(AssertionError) as context:
            row = Row(id=2, name="dipen")  # noqa
        with self.assertRaises(AssertionError) as context:
            row = Row(id=IntColumn(2), name="dipen")  # noqa
        with self.assertRaises(AssertionError) as context:
            row = Row(id=2, name=StrColumn("dipen"))  # noqa

    def test_fetch_row(self):
        # We insert row in a random location and then assert whether fetch_row correctly deserializes row or not
        row_v1 = Row(id=IntColumn(2378), name=StrColumn("dipen"))
        row_v1.left_child_offset = IntColumn(2378)
        row_v1.right_child_offset = IntColumn(415)
        location_of_insert = 239
        with open(DATABASE_FILE_NAME, "wb") as file:
            file.seek(location_of_insert)
            file.write(row_v1.serialize())

        row_v2 = Row.fetch_row(location_in_file=location_of_insert)
        self.assertEqual(row_v2.id.val, row_v1.id.val)
        self.assertEqual(row_v2.name.val, row_v1.name.val)
        self.assertEqual(row_v2.left_child_offset.val, row_v1.left_child_offset.val)
        self.assertEqual(row_v2.right_child_offset.val, row_v1.right_child_offset.val)
        self.assertEqual(row_v1.serialize(), row_v2.serialize())


class TestTableClass(unittest.TestCase):
    def setUp(self) -> None:
        with open(DATABASE_FILE_NAME, "wb"):
            pass
        self.student_table = Student()

    def test_existing_table_loads_correctly_from_disk(self):
        self.student_table.insert(row=Row(id=IntColumn(100), name=StrColumn("dipen")))
        self.student_table.insert(row=Row(id=IntColumn(200), name=StrColumn("mir")))

        # we load table again
        self.student_table = Student()
        self.assertEqual(self.student_table.row_count, 2)
        # We assert root row offset is correctly set
        self.assertEqual(self.student_table.root_row.offset, Student.SPACE_USED_FOR_SAVING_ROW_COUNT)


class TestInsertion(unittest.TestCase):
    def setUp(self) -> None:
        with open(DATABASE_FILE_NAME, "wb"):
            pass
        main.student_table = Student()

    # def tearDown(self) -> None:

    @staticmethod
    def raw_representation_of_row(id: int, name: str, left_child_offset=-1, right_child_offset=-1):
        return (
            id.to_bytes(4, byteorder="little")
            + name.ljust(32, "\0").encode("UTF-8")
            + struct.pack("i", left_child_offset)
            + struct.pack("i", right_child_offset)
        )

    @staticmethod
    def do_insert_command(id: int, name: str):
        query = f"""insert into students values ({id}, '{name}')"""
        return main.prepare_statement(query)

    def test_insert(self):
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=1, name="dipen"))
        self.assertEqual(main.student_table.row_count, 1)
        byte_to_be_inserted = self.raw_representation_of_row(1, "dipen")
        self.assertEqual(
            (1).to_bytes(Student.SPACE_USED_FOR_SAVING_ROW_COUNT, byteorder="little") + byte_to_be_inserted,
            main.student_table.raw_data(),
        )

    def test_multi_insert(self):
        logging.getLogger("table").setLevel(logging.DEBUG)
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=5, name="dipen"))
        self.assertEqual(main.student_table.row_count, 1)
        byte_to_be_inserted = self.raw_representation_of_row(5, "dipen")
        self.assertEqual(
            (1).to_bytes(Student.SPACE_USED_FOR_SAVING_ROW_COUNT, byteorder="little") + byte_to_be_inserted,
            main.student_table.raw_data(),
        )

        logger.info(f"Raw table data after one insert {main.student_table.raw_data()}")
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=8, name="chacha"))
        self.assertEqual(main.student_table.row_count, 2)

        raw_data_in_file_to_assert = (
            (2).to_bytes(Student.SPACE_USED_FOR_SAVING_ROW_COUNT, byteorder="little")
            + self.raw_representation_of_row(id=5, name="dipen", right_child_offset=46)
            + self.raw_representation_of_row(id=8, name="chacha")
        )
        self.assertEqual(raw_data_in_file_to_assert, main.student_table.raw_data())

        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=3, name="chacha part 2"))
        self.assertEqual(main.student_table.row_count, 3)

        raw_data_in_file_to_assert = (
            (3).to_bytes(Student.SPACE_USED_FOR_SAVING_ROW_COUNT, byteorder="little")
            + self.raw_representation_of_row(id=5, name="dipen", right_child_offset=46, left_child_offset=90)
            + self.raw_representation_of_row(id=8, name="chacha")
            + self.raw_representation_of_row(id=3, name="chacha part 2")
        )
        self.assertEqual(raw_data_in_file_to_assert, main.student_table.raw_data())

    def test_invalid_insert_is_blocked(self):
        self.assertEqual(PrepareStatementResult.SYNTAX_ERROR, self.do_insert_command(id="bad data", name="dipen"))
        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2**32, name="dipen")
        self.assertEqual("value should be less than 4294967296", context.exception.args[0])

        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2, name="d" * 33)
        self.assertEqual("Max 32 characters allowed", context.exception.args[0])


class TestBulkInsertion(unittest.TestCase):
    def setUp(self) -> None:
        with open(DATABASE_FILE_NAME, "wb"):
            pass
        main.student_table = Student()

    def test_insert_lotta_rows(self):
        num_rows = 10000
        t = time()
        logger.info(f"inserting {num_rows} rows")
        logger.info(f"Existing data {main.student_table.raw_data()}")
        for i in range(num_rows):
            main.student_table.insert(
                row=Row(id=IntColumn(random.randint(1, 1000000)), name=StrColumn(f"mir chacha{str(i)}"))
            )

        self.assertEqual(main.student_table.row_count, num_rows)

        # Now we assert the correctness of the tree, by fetching all child and checking whether they are sorted or not
        rows = main.student_table.binary_tree.traverse(root_row=main.student_table.root_row)
        row_ids = [row.id.val for row in rows]
        self.assertTrue(all(row_ids[i] <= row_ids[i + 1] for i in range(len(row_ids) - 1)))
        logger.info(f"Time took {time() - t}")


class TestInsertIntoBinaryTree(unittest.TestCase):
    def setUp(self) -> None:
        with open(DATABASE_FILE_NAME, "wb"):
            pass
        self.student_table = Student()

    def test_insert_row_into_binary_tree_and_assert__child_pointer_correctly_set(self):
        """
            we are gonna make a tree like

                    100
                 /       \
                 90        110
              /      \      /
              80      95    105
        """
        row1_to_insert = Row(id=IntColumn(100), name=StrColumn("dipen"))
        logger.info(f"Inserting {row1_to_insert=}")
        self.student_table.insert(row=row1_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(self.student_table.row_count, 1)

        row2_to_insert = Row(id=IntColumn(90), name=StrColumn("mir"))
        logger.info(f"Inserting {row2_to_insert=}")
        self.student_table.insert(row=row2_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(self.student_table.row_count, 2)
        self.assertEqual(row2_to_insert.parent, row1_to_insert)

        row3_to_insert = Row(id=IntColumn(80), name=StrColumn("shumroze"))
        logger.info(f"Inserting {row3_to_insert=}")
        self.student_table.insert(row=row3_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(self.student_table.row_count, 3)
        self.assertEqual(row3_to_insert.parent, row2_to_insert)

        row4_to_insert = Row(id=IntColumn(95), name=StrColumn("dawar shumroze"))
        logger.info(f"Inserting {row4_to_insert=}")
        self.student_table.insert(row=row4_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(row4_to_insert.parent, row2_to_insert)

        row5_to_insert = Row(id=IntColumn(110), name=StrColumn("dipendra bhatt"))
        logger.info(f"Inserting {row5_to_insert=}")
        self.student_table.insert(row=row5_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(row5_to_insert.parent, row1_to_insert)

        row6_to_insert = Row(id=IntColumn(105), name=StrColumn("dipendra"))
        logger.info(f"Inserting {row6_to_insert=}")
        self.student_table.insert(row=row6_to_insert)
        self.assertEqual(self.student_table.root_row, row1_to_insert)
        self.assertEqual(row6_to_insert.parent, row5_to_insert)
