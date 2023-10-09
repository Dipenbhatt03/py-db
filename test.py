import logging
import unittest

import main
from main import PrepareStatementResult
from src.config import DATABASE_FILE_NAME
from src.pager import pager
from src.row import IntColumn, Row, S2Int, StrColumn
from src.table import Student

logger = logging.getLogger(__name__)


# logging.getLogger("main").setLevel(logging.WARN)
# logging.getLogger("table").setLevel(logging.WARN)
# logging.getLogger("binary_tree").setLevel(logging.WARN)
class BaseTestClass(unittest.TestCase):
    def setUp(self) -> None:
        db_file = open(DATABASE_FILE_NAME, "w")
        db_file.close()
        self.student_table = Student()


class TestRowClass(BaseTestClass):
    def test_serialize_deserialize(self):
        row_v1 = Row(id=IntColumn(2378), name=StrColumn("dipen"))
        row_v1.left_child_offset = IntColumn(2378)
        row_v1.right_child_offset = IntColumn(415)
        row_v1.subtree_height = S2Int(3)
        row_v2 = Row.deserialize(row_v1.serialize())
        self.assertEqual(row_v2.id.val, row_v1.id.val)
        self.assertEqual(row_v2.name.val, row_v1.name.val)
        self.assertEqual(row_v2.left_child_offset.val, row_v1.left_child_offset.val)
        self.assertEqual(row_v2.right_child_offset.val, row_v1.right_child_offset.val)
        self.assertEqual(row_v2.subtree_height.val, 3)
        self.assertEqual(row_v1.serialize(), row_v2.serialize())

    def test_invalid_data_is_not_accepted(self):
        with self.assertRaises(AssertionError) as context:
            row = Row(id=2, name="dipen")  # noqa
        with self.assertRaises(AssertionError) as context:
            row = Row(id=IntColumn(2), name="dipen")  # noqa
        with self.assertRaises(AssertionError) as context:
            row = Row(id=2, name=StrColumn("dipen"))  # noqa

    def test_fetch_row(self):
        # We insert row and then assert whether fetch_row correctly deserializes row or not
        row_v1 = Row(id=IntColumn(2378), name=StrColumn("dipen"))
        row_v1.left_child_offset = IntColumn(2378)
        row_v1.right_child_offset = IntColumn(415)
        row_v1.subtree_height = S2Int(3)

        self.student_table.row_count = 0

        row_v1.offset = self.student_table.offset_for_a_new_row
        pager.page_flush(page_num=pager.page_write(row_v1.offset, row_v1.serialize()))

        row_v2 = Row.fetch_row(location_in_file=row_v1.offset)
        self.assertIsNotNone(row_v2)
        self.assertEqual(row_v2.id.val, row_v1.id.val)
        self.assertEqual(row_v2.name.val, row_v1.name.val)
        self.assertEqual(row_v2.left_child_offset.val, row_v1.left_child_offset.val)
        self.assertEqual(row_v2.right_child_offset.val, row_v1.right_child_offset.val)
        self.assertEqual(row_v2.subtree_height.val, row_v1.subtree_height.val)
        self.assertEqual(row_v1.serialize(), row_v2.serialize())


class TestInsertion(BaseTestClass):
    # def tearDown(self) -> None:

    @staticmethod
    def do_insert_command(id: int, name: str):
        query = f"""insert into students values ({id}, '{name}')"""
        return main.prepare_statement(query)

    def test_insert(self):
        self.assertEqual(PrepareStatementResult.SUCCESS, self.do_insert_command(id=1, name="dipen"))
        self.assertEqual(main.student_table.row_count, 1)

    def test_invalid_insert_is_blocked(self):
        self.assertEqual(PrepareStatementResult.SYNTAX_ERROR, self.do_insert_command(id="bad data", name="dipen"))
        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2**32, name="dipen")
        self.assertEqual("value should be less than 2147483648", context.exception.args[0])

        with self.assertRaises(AssertionError) as context:
            self.do_insert_command(id=2, name="d" * 33)
        self.assertEqual("Max 32 characters allowed", context.exception.args[0])


#
# class TestBulkInsertion(unittest.TestCase):
#     def setUp(self) -> None:
#         DATABASE_FD.truncate(0)
#         DATABASE_FD.flush()
#         self.student_table = Student()
#
#     def tearDown(self) -> None:
#         DATABASE_FD.truncate(0)
#         DATABASE_FD.flush()
#
#     def test_insert_lotta_rows(self):
#         num_rows = 65535
#         t = time()
#         logger.info(f"inserting {num_rows} rows")
#         logger.info(f"Existing data {main.student_table.raw_data()}")
#         row_ids_to_insert = []
#         for i in range(num_rows):
#             # logger.info(f"Inserting {i} record")
#             row_id = random.randint(1, 1000000)
#             # row_id = i
#             row_ids_to_insert.append(row_id)
#             self.student_table.insert(
#                 row=Row(id=IntColumn(row_id), name=StrColumn(f"mir chacha{str(i)}"))
#             )
#         row_ids_to_insert.sort()
#         self.assertEqual(self.student_table.row_count, num_rows)
#
#         # Now we assert the correctness of the tree, by fetching all child and checking whether they are sorted or not
#         rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#         row_ids = [row.id.val for row in rows]
#         self.assertTrue(all(row_ids[i] <= row_ids[i + 1] for i in range(len(row_ids) - 1)))
#         self.assertEqual(row_ids, row_ids_to_insert)
#         logger.info(f"Time took {time() - t}")
#
#
# class TestInsertIntoBinaryTree(unittest.TestCase):
#     def setUp(self) -> None:
#         DATABASE_FD.truncate(0)
#         DATABASE_FD.flush()
#         self.student_table = Student()
#
#     def tearDown(self) -> None:
#         DATABASE_FD.truncate(0)
#         DATABASE_FD.flush()
#
#     # def test_insert_row_into_binary_tree_and_assert_child_pointer_correctly_set(self):
#     #     """
#     #         we are gonna make a tree like
#     #
#     #                 100
#     #              /       \
#     #              90        110
#     #           /      \      /
#     #           80      95    105
#     #     """
#     #     row1_to_insert = Row(id=IntColumn(100), name=StrColumn("dipen"))
#     #     logger.info(f"Inserting {row1_to_insert=}")
#     #     self.student_table.insert(row=row1_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #     self.assertEqual(self.student_table.row_count, 1)
#     #
#     #     row2_to_insert = Row(id=IntColumn(90), name=StrColumn("mir"))
#     #     logger.info(f"Inserting {row2_to_insert=}")
#     #     self.student_table.insert(row=row2_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #     self.assertEqual(self.student_table.row_count, 2)
#     #
#     #     row3_to_insert = Row(id=IntColumn(80), name=StrColumn("shumroze"))
#     #     logger.info(f"Inserting {row3_to_insert=}")
#     #     self.student_table.insert(row=row3_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #     self.assertEqual(self.student_table.row_count, 3)
#     #
#     #     row4_to_insert = Row(id=IntColumn(95), name=StrColumn("dawar shumroze"))
#     #     logger.info(f"Inserting {row4_to_insert=}")
#     #     self.student_table.insert(row=row4_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #
#     #     row5_to_insert = Row(id=IntColumn(110), name=StrColumn("dipendra bhatt"))
#     #     logger.info(f"Inserting {row5_to_insert=}")
#     #     self.student_table.insert(row=row5_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #
#     #     row6_to_insert = Row(id=IntColumn(105), name=StrColumn("dipendra"))
#     #     logger.info(f"Inserting {row6_to_insert=}")
#     #     self.student_table.insert(row=row6_to_insert)
#     #     self.assertEqual(self.student_table.root_row, row1_to_insert)
#     #
#     #     all_rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#     #     logger.info(f"{all_rows=}")
#     #     row_ids = [row.id.val for row in all_rows]
#     #     self.assertEqual(row_ids, [80, 90, 95, 100, 105, 110])
#
#     def test_left_rotation_of_avl_tree(self):
#         """
#         In this test case we will only insert data that goes to the right side of the tree
#         """
#
#         if not isinstance(self.student_table.binary_tree, AvlBinaryTree):
#             return
#         row1_to_insert = Row(id=IntColumn(1), name=StrColumn("dipen"))
#         logger.info(f"Inserting {row1_to_insert=}")
#         self.student_table.insert(row=row1_to_insert)
#         self.assertEqual(self.student_table.root_row, row1_to_insert)
#         self.assertEqual(self.student_table.row_count, 1)
#         self.assertEqual(row1_to_insert.subtree_height, 1)
#
#         row2_to_insert = Row(id=IntColumn(2), name=StrColumn("mir"))
#         logger.info(f"Inserting {row2_to_insert=}")
#         self.student_table.insert(row=row2_to_insert)
#         self.assertEqual(self.student_table.root_row, row1_to_insert)
#         self.assertEqual(self.student_table.row_count, 2)
#         self.assertEqual(row1_to_insert.subtree_height, 2)
#         self.assertEqual(row2_to_insert.subtree_height, 1)
#
#         row3_to_insert = Row(id=IntColumn(3), name=StrColumn("shumroze"))
#         logger.info(f"Inserting {row3_to_insert=}")
#         self.student_table.insert(row=row3_to_insert)
#         self.assertEqual(self.student_table.row_count, 3)
#         self.assertEqual(self.student_table.root_row, row2_to_insert)
#         self.assertEqual(row1_to_insert.subtree_height, 1)
#         self.assertEqual(row2_to_insert.subtree_height, 2)
#         self.assertEqual(row3_to_insert.subtree_height, 1)
#
#         row4_to_insert = Row(id=IntColumn(4), name=StrColumn("dawar shumroze"))
#         logger.info(f"Inserting {row4_to_insert=}")
#         self.student_table.insert(row=row4_to_insert)
#         self.assertEqual(row1_to_insert.subtree_height, 1)
#         self.assertEqual(row2_to_insert.subtree_height, 3)
#         self.assertEqual(row3_to_insert.subtree_height, 2)
#         self.assertEqual(row4_to_insert.subtree_height, 1)
#         self.assertEqual(self.student_table.root_row, row2_to_insert)
#
#         row5_to_insert = Row(id=IntColumn(5), name=StrColumn("dipendra bhatt"))
#         logger.info(f"Inserting {row5_to_insert=}")
#         self.student_table.insert(row=row5_to_insert)
#         self.assertEqual(row2_to_insert.subtree_height, 3)
#         #
#         row6_to_insert = Row(id=IntColumn(6), name=StrColumn("dipendra"))
#         logger.info(f"Inserting {row6_to_insert=}")
#         self.student_table.insert(row=row6_to_insert)
#
#         all_rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#         logger.info(f"{all_rows=}")
#         row_ids = [row.id.val for row in all_rows]
#         self.assertEqual(row_ids, [1, 2, 3, 4, 5, 6])
#
#         """
#             Asserting the child pointers of every node
#             Basically tree should be like
#
#                         4
#                     /       \
#                 2               5
#             /       \               \
#             1        3                  6
#
#         """
#         self.assertEqual(self.student_table.root_row, row4_to_insert)
#         self.assertEqual(row4_to_insert.left_child, row2_to_insert)
#         self.assertEqual(row4_to_insert.right_child, row5_to_insert)
#         self.assertEqual(row2_to_insert.left_child, row1_to_insert)
#         self.assertEqual(row2_to_insert.right_child, row3_to_insert)
#         self.assertEqual(row5_to_insert.right_child, row6_to_insert)
#
#         self.assertIsNone(row6_to_insert.left_child)
#         self.assertIsNone(row6_to_insert.right_child)
#
#         self.assertIsNone(row1_to_insert.left_child)
#         self.assertIsNone(row1_to_insert.right_child)
#
#         self.assertIsNone(row3_to_insert.left_child)
#         self.assertIsNone(row3_to_insert.right_child)
#
#     def test_right_rotation_of_avl_tree(self):
#         """
#         In this test case we will only insert data that goes to the right side of the tree
#         """
#         row6_to_insert = Row(id=IntColumn(6), name=StrColumn("dipen"))
#         self.student_table.insert(row=row6_to_insert)
#
#         row5_to_insert = Row(id=IntColumn(5), name=StrColumn("mir"))
#         self.student_table.insert(row=row5_to_insert)
#
#         row4_to_insert = Row(id=IntColumn(4), name=StrColumn("shumroze"))
#         self.student_table.insert(row=row4_to_insert)
#
#         row3_to_insert = Row(id=IntColumn(3), name=StrColumn("ojasvi"))
#         self.student_table.insert(row=row3_to_insert)
#
#         row2_to_insert = Row(id=IntColumn(2), name=StrColumn("sid"))
#         self.student_table.insert(row=row2_to_insert)
#         #
#         row1_to_insert = Row(id=IntColumn(1), name=StrColumn("dipen"))
#         self.student_table.insert(row=row1_to_insert)
#
#         all_rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#         logger.info(f"{all_rows=}")
#         row_ids = [row.id.val for row in all_rows]
#         self.assertEqual(row_ids, [1, 2, 3, 4, 5, 6])
#
#         """
#                     Asserting the child pointers of every node
#                     Basically tree should be like
#
#                                 3
#                             /       \
#                         2               5
#                     /                  /     \
#                     1               4          6
#
#                 """
#         self.assertEqual(self.student_table.root_row, row3_to_insert)
#         self.assertEqual(row3_to_insert.left_child, row2_to_insert)
#         self.assertEqual(row3_to_insert.right_child, row5_to_insert)
#         self.assertEqual(row2_to_insert.left_child, row1_to_insert)
#         self.assertEqual(row5_to_insert.right_child, row6_to_insert)
#         self.assertEqual(row5_to_insert.left_child, row4_to_insert)
#
#         self.assertIsNone(row6_to_insert.left_child)
#         self.assertIsNone(row6_to_insert.right_child)
#
#         self.assertIsNone(row1_to_insert.left_child)
#         self.assertIsNone(row1_to_insert.right_child)
#
#         self.assertIsNone(row4_to_insert.left_child)
#         self.assertIsNone(row4_to_insert.right_child)
#
#     def test_right_left_rotation_of_avl_tree(self):
#         row5_to_insert = Row(id=IntColumn(5), name=StrColumn("dipen"))
#         self.student_table.insert(row=row5_to_insert)
#
#         row8_to_insert = Row(id=IntColumn(8), name=StrColumn("mir"))
#         self.student_table.insert(row=row8_to_insert)
#
#         row12_to_insert = Row(id=IntColumn(12), name=StrColumn("shumroze"))
#         self.student_table.insert(row=row12_to_insert)
#
#         row11_to_insert = Row(id=IntColumn(11), name=StrColumn("ojasvi"))
#         self.student_table.insert(row=row11_to_insert)
#
#         row10_to_insert = Row(id=IntColumn(10), name=StrColumn("sid"))
#         self.student_table.insert(row=row10_to_insert)
#
#         row9_to_insert = Row(id=IntColumn(9), name=StrColumn("sid"))
#         self.student_table.insert(row=row9_to_insert)
#
#         all_rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#         logger.info(f"{all_rows=}")
#         row_ids = [row.id.val for row in all_rows]
#         self.assertEqual(row_ids, [5, 8, 9, 10, 11, 12])
#
#         """
#                             Asserting the child pointers of every node
#                             Basically tree should be like
#
#                                         10
#                                     /       \
#                                 8               11
#                             /     \                  \
#                             5      9                   12
#
#                         """
#         self.assertEqual(self.student_table.root_row, row10_to_insert)
#         self.assertEqual(row10_to_insert.left_child, row8_to_insert)
#         self.assertEqual(row10_to_insert.right_child, row11_to_insert)
#
#         self.assertEqual(row8_to_insert.left_child, row5_to_insert)
#         self.assertEqual(row8_to_insert.right_child, row9_to_insert)
#
#         self.assertEqual(row11_to_insert.right_child, row12_to_insert)
#
#         self.assertIsNone(row5_to_insert.left_child)
#         self.assertIsNone(row5_to_insert.right_child)
#
#         self.assertIsNone(row9_to_insert.left_child)
#         self.assertIsNone(row9_to_insert.right_child)
#
#         self.assertIsNone(row12_to_insert.left_child)
#         self.assertIsNone(row12_to_insert.right_child)
#
#     def test_left_right_rotation_of_avl_tree(self):
#         row12_to_insert = Row(id=IntColumn(12), name=StrColumn("dipen"))
#         self.student_table.insert(row=row12_to_insert)
#
#         row11_to_insert = Row(id=IntColumn(11), name=StrColumn("mir"))
#         self.student_table.insert(row=row11_to_insert)
#
#         row7_to_insert = Row(id=IntColumn(7), name=StrColumn("shumroze"))
#         self.student_table.insert(row=row7_to_insert)
#
#         row8_to_insert = Row(id=IntColumn(8), name=StrColumn("ojasvi"))
#         self.student_table.insert(row=row8_to_insert)
#
#         row9_to_insert = Row(id=IntColumn(9), name=StrColumn("sid"))
#         self.student_table.insert(row=row9_to_insert)
#
#         row10_to_insert = Row(id=IntColumn(10), name=StrColumn("sid"))
#         self.student_table.insert(row=row10_to_insert)
#
#         all_rows = self.student_table.binary_tree.traverse(root_row=self.student_table.root_row)
#         logger.info(f"{all_rows=}")
#         row_ids = [row.id.val for row in all_rows]
#         self.assertEqual(row_ids, [7, 8, 9, 10, 11, 12])
#
#         """
#                             Asserting the child pointers of every node
#                             Basically tree should be like
#
#                                         10
#                                     /       \
#                                 8               11
#                             /     \                  \
#                             5      9                   12
#
#                         """
#         self.assertEqual(self.student_table.root_row, row9_to_insert)
#         self.assertEqual(row9_to_insert.left_child, row8_to_insert)
#         self.assertEqual(row9_to_insert.right_child, row11_to_insert)
#
#         self.assertEqual(row8_to_insert.left_child, row7_to_insert)
#
#         self.assertEqual(row11_to_insert.right_child, row12_to_insert)
#         self.assertEqual(row11_to_insert.left_child, row10_to_insert)
#
#         self.assertIsNone(row7_to_insert.left_child)
#         self.assertIsNone(row7_to_insert.right_child)
#
#         self.assertIsNone(row10_to_insert.left_child)
#         self.assertIsNone(row10_to_insert.right_child)
#
#         self.assertIsNone(row12_to_insert.left_child)
#         self.assertIsNone(row12_to_insert.right_child)
