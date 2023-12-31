import logging
from typing import Optional, cast

from src.row import Row, SmallInt

logger = logging.getLogger(__name__)


class BinaryTree:
    """
    A class that implements binary tree insertion and search.
    The way to use this class is, a table instantiates this class as one of its property and during insert,
    calls the insert of this class which returns the same row while setting the row to be inserted parent
    and updating the left right pointers of the parent.

    """

    def __init__(self, table):
        self.meta_info = {}

        self._row_to_insert: Optional[Row] = None
        self.table = table

    def insert(self, row_to_insert: Row, root_row: Row):
        logger.debug(f"{row_to_insert=} {root_row=}")
        if root_row is None:
            root_row = row_to_insert

        elif row_to_insert.id.val <= root_row.id.val:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.left_child)
            del root_row.left_child  # noqa
            root_row.left_child_offset.val = row_to_insert.offset

        else:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.right_child)
            del root_row.right_child  # noqa
            root_row.right_child_offset.val = row_to_insert.offset

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
            logger.debug(f"{root_row=} {row_id_to_search=} {depth=}")

            rows_traversed = self.traverse(
                root_row=root_row.left_child,
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )
            rows_traversed.append(root_row)

            rows_traversed = self.traverse(
                root_row=root_row.right_child,
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )
        elif row_id_to_search <= root_row.id.val:
            # if less than we search in left tree
            if row_id_to_search == root_row.id.val:
                rows_traversed.append(root_row)
            rows_traversed = self.traverse(
                root_row=root_row.left_child,
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )

        else:
            # if value is greater we search in right tree
            rows_traversed = self.traverse(
                root_row=root_row.right_child,
                rows_traversed=rows_traversed,
                row_id_to_search=row_id_to_search,
                depth=depth + 1,
            )

        return rows_traversed


class AvlBinaryTree(BinaryTree):
    def right_rotate(self, root_row: Row) -> Row:
        """
        NOTE: Care needs to be taken when we are fetching left or right child.
        Because, the property left_child/right_child of Row class fetch the children from
        disk. Whereas the changes happening in this function or the one calling it i.e insert are
        not yet flushed to disk and they are in memory. So a existing calculation done on a row instance
        needs to be reused, care should be taken when calling the property function.

        Although to protect from cases like these, we have added cached_property decorator to
        left_child/right_child assuming its gonna return the same memory location everytime.

        """
        logger.debug(f"Right rotation around {root_row=}")
        left_child_row = root_row.left_child
        root_row.left_child_offset = left_child_row.right_child_offset
        left_child_row.right_child_offset = root_row.offset

        # updating the heights
        root_row.subtree_height = SmallInt(max(root_row.right_subtree_height, root_row.left_subtree_height) + 1)

        left_child_row.subtree_height = SmallInt(
            max(
                left_child_row.left_subtree_height,
                # Pay attention here, we dont use left_child_row.right_child.offset which should technically be
                # the previous root row, but we haven't flushed the change to disk and doing that would return
                # something else entirely. So we reuse the root_row variable over which calculation have already
                # happened.
                root_row.subtree_height,
            )
            + 1
        )

        self.table.dirty_rows.add(left_child_row)
        self.table.dirty_rows.add(root_row)

        return left_child_row

    def left_rotate(self, root_row: Row) -> Row:
        logger.debug(f"Left rotation around {root_row=}")
        # Same care needs to be taken during left rotation as is noted down in right rotation
        right_child_row = root_row.right_child
        root_row.right_child_offset = right_child_row.left_child_offset
        right_child_row.left_child_offset = root_row.offset

        # updating heights
        root_row.subtree_height = SmallInt(max(root_row.right_subtree_height, root_row.left_subtree_height) + 1)

        right_child_row.subtree_height = SmallInt(
            max(right_child_row.right_subtree_height, root_row.subtree_height) + 1
        )

        self.table.dirty_rows.add(right_child_row)
        self.table.dirty_rows.add(root_row)
        return right_child_row

    def insert(self, row_to_insert: Row, root_row: Row):
        # sourcery skip: hoist-similar-statement-from-if, hoist-statement-from-if, remove-pass-body
        logger.debug(f"{row_to_insert=} {root_row=}")

        if root_row is None:
            root_row = row_to_insert
            self.table.dirty_rows.add(root_row)

        elif row_to_insert.id.val <= root_row.id.val:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.left_child)
            if root_row.left_child_offset != row_to_insert.offset:
                root_row.left_child_offset = row_to_insert.offset

                self.table.dirty_rows.add(root_row)
            # row_to_insert.parent = root_row
            root_row.subtree_height = SmallInt(max(root_row.subtree_height, root_row.left_subtree_height + 1))
        else:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.right_child)
            if root_row.right_child_offset != row_to_insert.offset:
                root_row.right_child_offset = row_to_insert.offset

                self.table.dirty_rows.add(root_row)
            # row_to_insert.parent = root_row
            root_row.subtree_height = SmallInt(max(root_row.subtree_height, root_row.right_subtree_height + 1))

        balance_factor = root_row.left_subtree_height - root_row.right_subtree_height
        if balance_factor > 1:
            # Left imbalance case, Now we figure whether this is a left-left case or left-right case by
            # comparing the id of inserted row with the left child of the root row

            if cast(Row, self._row_to_insert).id.val <= root_row.left_child.id.val:
                # a left-left case
                root_row = self.right_rotate(root_row=root_row)
            else:
                # a left-right case
                root_row.left_child_offset = self.left_rotate(root_row=root_row.left_child).offset
                root_row = self.right_rotate(root_row=root_row)

        elif balance_factor < -1:
            # right imbalance case, Now we figure out whether this is a right-right case or right-left case
            # by comparing the id of inserted row with right child of the root row
            if cast(Row, self._row_to_insert).id.val <= root_row.right_child.id.val:
                # a Right-left case
                root_row.right_child_offset = self.right_rotate(root_row=root_row.right_child).offset
                root_row = self.left_rotate(root_row=root_row)
            else:
                # A Right-Right case
                root_row = self.left_rotate(root_row=root_row)

        return root_row
