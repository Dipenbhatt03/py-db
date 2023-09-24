import logging
import unittest
from time import time

import config  # noqa
import main
from main import PrepareStatementResult, do_meta_command, flush_to_disk

logger = logging.getLogger(__name__)
logging.getLogger("main").setLevel(logging.INFO)


class TestBasicInsertion(unittest.TestCase):
    def setUp(self) -> None:
        with open("dipen.db", "wb"):
            pass

    def raw_representation_of_row(self, id: int, name: str):
        return id.to_bytes(4, byteorder="little") + name.ljust(32, '\0').encode("UTF-8")

    def do_insert_command(self, id: int, name: str):
        query = f"""insert into students values ({id}, '{name}')"""
        return main.prepare_statement(query)

    def test_insert(self):
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=1, name="dipen"))
        byte_to_be_inserted = self.raw_representation_of_row(1, "dipen")
        self.assertEqual(byte_to_be_inserted, main.student_table.raw_data)

    def test_multi_insert(self):
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=1, name="dipen"))
        byte_to_be_inserted = self.raw_representation_of_row(1, "dipen")
        self.assertEqual(byte_to_be_inserted, main.student_table.raw_data)
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=2, name="chacha"))
        byte_to_be_inserted += self.raw_representation_of_row(id=2, name="chacha")
        self.assertEqual(byte_to_be_inserted, main.student_table.raw_data)

    def test_invalid_insert_is_blocked(self):
        self.assertEqual(PrepareStatementResult.SYNTAX_ERROR, self.do_insert_command(id="bad data", name="dipen"))
        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2 ** 32, name="dipen")
        self.assertEqual('value should be less than 4294967296', context.exception.args[0])

        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2, name="d" * 33)
        self.assertEqual("Max 32 characters allowed", context.exception.args[0])

    def test_insert_lotta_rows(self):
        logger.info("inserting 1000 rows")
        byte_to_be_expected_to_insert = b''
        t = time()
        for i in range(100000):
            self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(
                id=i, name=f"chacha{str(i)}"
            ))
            byte_to_be_expected_to_insert += self.raw_representation_of_row(id=i, name=f"chacha{str(i)}")
            self.assertEqual(byte_to_be_expected_to_insert, main.student_table.raw_data)
        # flush_to_disk()
        logger.info(f"Time took {time() - t}")
