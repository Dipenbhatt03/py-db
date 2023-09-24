import logging
import struct
from abc import ABC, abstractmethod
from functools import lru_cache
from typing import Any, Optional, Self, Union, cast

import config  # noqa
from config import DATABASE_FILE_NAME

logger = logging.getLogger(__name__)


class Column(ABC):
    """
    OFFSET_FROM_WHERE_DATA_STARTS is the relative point in a raw data, from where this columns data starts.
    This assumes that columns for a table has a fixed ordering i.e first IntCol and then StrCol
    Obviously this a gross oversimplification of actual data is saved, but for now we work with this.
    """

    OFFSET_FROM_WHERE_DATA_STARTS: Optional[int] = None

    def __init__(self, val: Any):
        self.val = val
        self.val = self.validate_val()

    def __str__(self):
        return str(self.val)

    def __repr__(self):
        if self.val:
            return self.__str__()
        return super().__repr__()

    @abstractmethod
    def validate_val(self) -> Union[int, str]:
        ...

    @abstractmethod
    def serialize(self):
        ...

    @classmethod
    @abstractmethod
    def deserialize(cls, raw_byte_data):
        ...


class IntColumn(Column):
    SIZE_IN_BYTES = 4  # That 24 is cpython implementation overhead for int
    OFFSET_FROM_WHERE_DATA_STARTS = 0

    # def __init__(self, val: int):
    #     super().__init__(val=val)

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(cast(int, self.val))  # the cast is used for static type checker to infer type
        # 2**32 - 1 is the max number that can be represented by a 4 byte signed int
        assert self.val < 2**32, f"value should be less than {2 ** 32}"
        return self.val

    def serialize(self):
        return struct.pack("i", self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(val=struct.unpack("i", raw_byte_data)[0])


class StrColumn(Column):
    """

    Right now str columne will just have 4 bytes space
    """

    SIZE_IN_BYTES = 32  # the 49 part is the cpython implementation overhead for string type
    OFFSET_FROM_WHERE_DATA_STARTS = IntColumn.SIZE_IN_BYTES

    def __init__(self, val: str):
        super().__init__(val=val)

    def validate_val(self) -> str:
        assert isinstance(self.val, str), "str value expected"
        assert len(self.val) < self.SIZE_IN_BYTES, "Max 32 characters allowed"

        return self.val

    def serialize(self):
        return struct.pack(f"{self.SIZE_IN_BYTES}s", self.val.ljust(32, "\0").encode("UTF-8"))

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        logger.debug(f"str deserialize {raw_byte_data=}")
        unpacked_data = struct.unpack(f"{cls.SIZE_IN_BYTES}s", raw_byte_data)[0]

        return cls(val=unpacked_data.decode("UTF-8").rstrip("\0"))


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

    def __init__(self, id: IntColumn, name: StrColumn):
        assert isinstance(id, IntColumn)
        assert isinstance(name, StrColumn)
        self.id = id
        self.name = name
        self.offset: Optional[int] = None
        self.parent: Optional[Self] = None
        self.left_child_offset: IntColumn = IntColumn(-1)
        self.right_child_offset: IntColumn = IntColumn(-1)

    def __str__(self):
        return f"id={self.id} name={self.name}"

    def __repr__(self):
        return f"id={self.id} name={self.name}"

    def __eq__(self, other):
        if isinstance(other, type(self)):
            # right now, we assume that if two rows offset is same, they hold the same value since we don't
            # directly manipulate file data and fetch row via a tested fetch_row function
            return self.offset == other.offset
        return super().__eq__(other)

    @classmethod
    def size(cls):
        return (
            IntColumn.SIZE_IN_BYTES
            + StrColumn.SIZE_IN_BYTES
            + cls.LEFT_POINTER_SIZE_IN_BYTES
            + cls.RIGHT_POINTER_SIZE_IN_BYTES
        )

    def serialize(self):
        return (
            self.id.serialize()
            + self.name.serialize()
            + self.left_child_offset.serialize()
            + self.right_child_offset.serialize()
        )

    @classmethod
    def deserialize(cls, raw_byte_data: bytes) -> Self:
        logger.debug(f"{raw_byte_data=}")
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

        id_instance = IntColumn.deserialize(raw_byte_data=raw_id_bytes)
        name_instance = StrColumn.deserialize(raw_byte_data=raw_name_bytes)
        left_child_offset_bytes_instance = IntColumn.deserialize(raw_byte_data=left_child_offset_bytes)
        right_child_offset_bytes_instance = IntColumn.deserialize(raw_byte_data=right_child_offset_bytes)
        row = cls(id=id_instance, name=name_instance)
        row.left_child_offset = left_child_offset_bytes_instance
        row.right_child_offset = right_child_offset_bytes_instance
        return row

    @classmethod
    @lru_cache(maxsize=1000)
    def fetch_row(cls, location_in_file: int) -> Optional[Self]:
        if location_in_file < 0:
            return None
        with open(DATABASE_FILE_NAME, "rb") as file:
            file.seek(location_in_file)
            row_raw_data = file.read(cls.size())
            row = cls.deserialize(raw_byte_data=row_raw_data)
            row.offset = location_in_file
            return row
