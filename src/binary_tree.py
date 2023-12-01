import logging
from typing import Optional, cast

from src.data_types import S2Int
from src.row import Row

logger = logging.getLogger(__name__)

class AvlBinaryTree:
    def __init__(self):
        self.meta_info = {}
        self._row_to_insert: Optional[Row] = None

    @staticmethod
    def right_rotate(root_row: Optional[Row]) -> Optional[Row]:
        logger.debug(f"Right rotation around {root_row=}")
        if root_row is None:
            return root_row

        left_child_row = cast(Row, root_row.left_child)  # casting to make mypy happy
        root_row.left_child_offset = left_child_row.right_child_offset
        left_child_row.right_child_offset = root_row.offset

        # updating the heights
        root_row.subtree_height = S2Int(max(root_row.right_subtree_height, root_row.left_subtree_height) + 1)

        left_child_row.subtree_height = S2Int(
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
        left_child_row.write_to_page()
        root_row.write_to_page()

        return left_child_row

    @staticmethod
    def left_rotate(root_row: Optional[Row]) -> Optional[Row]:
        logger.debug(f"Left rotation around {root_row=}")
        if root_row is None:
            return root_row

        right_child_row = cast(Row, root_row.right_child)  # casting to make mypy happy
        root_row.right_child_offset = right_child_row.left_child_offset
        right_child_row.left_child_offset = root_row.offset

        # updating heights
        root_row.subtree_height = S2Int(max(root_row.right_subtree_height, root_row.left_subtree_height) + 1)

        right_child_row.subtree_height = S2Int(max(right_child_row.right_subtree_height, root_row.subtree_height) + 1)
        right_child_row.write_to_page()
        root_row.write_to_page()
        return right_child_row

    def insert(self, row_to_insert: Row, root_row: Optional[Row]) -> Row:
        # sourcery skip: hoist-similar-statement-from-if, hoist-statement-from-if, remove-pass-body
        logger.debug(f"{row_to_insert=} {root_row=}")
        if root_row is None:
            root_row = row_to_insert
            root_row.write_to_page()

        elif row_to_insert.id.val <= root_row.id.val:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.left_child)
            if root_row.left_child_offset != row_to_insert.offset:
                root_row.left_child_offset = row_to_insert.offset

            root_row.subtree_height = S2Int(max(root_row.subtree_height, root_row.left_subtree_height + 1))
            root_row.write_to_page()
        else:
            row_to_insert = self.insert(row_to_insert=row_to_insert, root_row=root_row.right_child)
            if root_row.right_child_offset != row_to_insert.offset:
                root_row.right_child_offset = row_to_insert.offset
            root_row.subtree_height = S2Int(max(root_row.subtree_height, root_row.right_subtree_height + 1))
            root_row.write_to_page()

        balance_factor = root_row.left_subtree_height - root_row.right_subtree_height
        if balance_factor > 1:
            # Left imbalance case, Now we figure whether this is a left-left case or left-right case by
            # comparing the id of inserted row with the left child of the root row

            if cast(Row, self._row_to_insert).id.val <= root_row.left_child.id.val:
                # a left-left case
                root_row = self.right_rotate(root_row=root_row)
            else:
                if temp_root := self.left_rotate(root_row=root_row.left_child):
                    root_row.left_child_offset = temp_root.offset
                root_row = self.right_rotate(root_row=root_row)

        elif balance_factor < -1:
            # right imbalance case, Now we figure out whether this is a right-right case or right-left case
            # by comparing the id of inserted row with right child of the root row
            if cast(Row, self._row_to_insert).id.val <= root_row.right_child.id.val:
                if temp_root := self.right_rotate(root_row=root_row.right_child):
                    root_row.right_child_offset = temp_root.offset
                root_row = self.left_rotate(root_row=root_row)
            else:
                # A Right-Right case
                root_row = self.left_rotate(root_row=root_row)

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
