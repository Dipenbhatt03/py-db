import logging
from typing import Optional, Self

from src.data_types import S2Int, S4Int, Str
from src.pager import pager

logger = logging.getLogger(__name__)


class IntColumn(S4Int):
    OFFSET_FROM_WHERE_DATA_STARTS = 0


class StrColumn(Str):
    """

    Right now str column will just have 4 bytes space
    """

    OFFSET_FROM_WHERE_DATA_STARTS = IntColumn.SIZE_IN_BYTES


class Row:
    """
    Row file format will be like

    (4 bytes for id col)(32 bytes for name column)(4 bytes for left child address)(4 bytes for left child address)

    we use 4 byte for left and right child denotion coz, right now our table is limited to 65535 rows
    and one row payload has the size of 36 bytes. So just raw data will be max  65535 * 36 = 2359260 bytes
    i.e 2 Mb. So if we use 4 bytes for pointing out children, that effectively makes our row size 44 bytes
    and 65535 * 44 = 2883540 memory location. Which can be easily handled by the 4 byte integers.

    """

    LEFT_POINTER_SIZE_IN_BYTES = 4
    RIGHT_POINTER_SIZE_IN_BYTES = 4

    SUBTREE_HEIGHT_IN_BYTES = 2

    OFFSET_WHERE_SUBTREE_HEIGHT_DATA_STARTS = (
        StrColumn.OFFSET_FROM_WHERE_DATA_STARTS
        + StrColumn.SIZE_IN_BYTES
        + LEFT_POINTER_SIZE_IN_BYTES
        + RIGHT_POINTER_SIZE_IN_BYTES
    )

    def __init__(self, id: IntColumn, name: StrColumn):
        assert isinstance(id, IntColumn)
        assert isinstance(name, StrColumn)
        self.id = id
        self.name = name
        self.offset: S4Int = S4Int(-1)
        self.left_child_offset: S4Int = S4Int(-1)
        self.right_child_offset: S4Int = S4Int(-1)
        self.subtree_height: S2Int = S2Int(1)

    def __str__(self):
        return f"(id={self.id} name={self.name} offset={self.offset})"

    def __repr__(self):
        return f"(id={self.id} name={self.name} offset={self.offset})"

    def __hash__(self):
        assert self.offset is not None
        return hash(str(self.offset) + str(self.subtree_height))

    def __eq__(self, other):
        if isinstance(other, type(self)):
            # right now, we assume that if two rows offset is same, they hold the same value since we don't
            # directly manipulate file data and fetch row via a tested fetch_row function
            return self.offset == other.offset
        return super().__eq__(other)

    @property
    def right_child(self) -> Optional[Self]:
        return self.fetch_row(location_in_file=self.right_child_offset)

    @property
    def left_child(self) -> Optional[Self]:
        return self.fetch_row(location_in_file=self.left_child_offset)

    @property
    def left_subtree_height(self):
        return self.left_child.subtree_height if self.left_child else 0

    @property
    def right_subtree_height(self):
        return self.right_child.subtree_height if self.right_child else 0

    @classmethod
    def size(cls):
        return (
            IntColumn.SIZE_IN_BYTES
            + StrColumn.SIZE_IN_BYTES
            + cls.LEFT_POINTER_SIZE_IN_BYTES
            + cls.RIGHT_POINTER_SIZE_IN_BYTES
            + cls.SUBTREE_HEIGHT_IN_BYTES
        )

    def serialize(self):
        return (
            self.id.serialize()
            + self.name.serialize()
            + self.left_child_offset.serialize()
            + self.right_child_offset.serialize()
            + self.subtree_height.serialize()
        )

    @classmethod
    def deserialize(cls, raw_byte_data: bytes) -> Self:
        # logger.debug(f"{raw_byte_data=}")
        raw_id_bytes = raw_byte_data[
            IntColumn.OFFSET_FROM_WHERE_DATA_STARTS : IntColumn.OFFSET_FROM_WHERE_DATA_STARTS + IntColumn.SIZE_IN_BYTES
        ]
        raw_name_bytes = raw_byte_data[
            StrColumn.OFFSET_FROM_WHERE_DATA_STARTS : StrColumn.OFFSET_FROM_WHERE_DATA_STARTS + StrColumn.SIZE_IN_BYTES
        ]
        child_pointer_offset = StrColumn.OFFSET_FROM_WHERE_DATA_STARTS + StrColumn.SIZE_IN_BYTES
        left_child_offset_bytes = raw_byte_data[
            child_pointer_offset : child_pointer_offset + cls.LEFT_POINTER_SIZE_IN_BYTES
        ]
        right_child_offset_bytes = raw_byte_data[
            child_pointer_offset
            + cls.LEFT_POINTER_SIZE_IN_BYTES : child_pointer_offset
            + cls.LEFT_POINTER_SIZE_IN_BYTES
            + cls.RIGHT_POINTER_SIZE_IN_BYTES
        ]

        subtree_height_offset_bytes = raw_byte_data[
            cls.OFFSET_WHERE_SUBTREE_HEIGHT_DATA_STARTS : cls.OFFSET_WHERE_SUBTREE_HEIGHT_DATA_STARTS
            + cls.SUBTREE_HEIGHT_IN_BYTES
        ]

        id_instance = IntColumn.deserialize(raw_byte_data=raw_id_bytes)
        name_instance = StrColumn.deserialize(raw_byte_data=raw_name_bytes)
        left_child_offset_bytes_instance = S4Int.deserialize(raw_byte_data=left_child_offset_bytes)
        right_child_offset_bytes_instance = S4Int.deserialize(raw_byte_data=right_child_offset_bytes)
        subtree_height_instance = S2Int.deserialize(raw_byte_data=subtree_height_offset_bytes)

        row = cls(id=id_instance, name=name_instance)
        row.left_child_offset = left_child_offset_bytes_instance
        row.right_child_offset = right_child_offset_bytes_instance
        row.subtree_height = subtree_height_instance
        return row

    @classmethod
    # @lru_cache(maxsize=10000)
    def fetch_row(cls, location_in_file: int) -> Optional[Self]:
        if location_in_file < 0:
            return None
        page_num, page_offset = pager.get_pager_location_from_offset(offset=location_in_file)
        page = pager.get_page(page_num=page_num)
        return cls.deserialize(page.data[page_offset : page_offset + Row.size()])
