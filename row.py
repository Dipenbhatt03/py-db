import logging
import struct
from abc import ABC, abstractmethod
from typing import Union

import config  # noqa

logger = logging.getLogger(__name__)

class Column(ABC):

    def __init__(self, val: Union[int, str]):
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

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(self.val)
        # 2**32 - 1 is the max number that can be represented by a 4 byte signed int
        assert self.val < 2 ** 32, f"value should be less than {2 ** 32}"
        return self.val

    def serialize(self):
        return struct.pack('i', self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(val=struct.unpack('i', raw_byte_data)[0])


class StrColumn(Column):
    """

        Right now str columne will just have 4 bytes space
    """
    SIZE_IN_BYTES = 32  # the 49 part is the cpython implementation overhead for string type

    def validate_val(self) -> str:
        assert isinstance(self.val, str), "str value expected"
        assert len(self.val) < self.SIZE_IN_BYTES, "Max 32 characters allowed"

        return self.val

    def serialize(self):
        return struct.pack(f'{self.SIZE_IN_BYTES}s', self.val.ljust(32, '\0').encode("UTF-8"))

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        logger.debug(f"str deserialize {raw_byte_data=}")
        unpacked_data = struct.unpack(f'{cls.SIZE_IN_BYTES}s', raw_byte_data)[0]

        return cls(val=unpacked_data.decode("UTF-8").rstrip('\0'))


class Row:
    def __init__(self, id: IntColumn, name: StrColumn):
        self.id = id
        self.name = name

    def __str__(self):
        return f"id={self.id} name={self.name}"

    @staticmethod
    def size():
        return IntColumn.SIZE_IN_BYTES + StrColumn.SIZE_IN_BYTES

    def serialize(self):
        return self.id.serialize() + self.name.serialize()

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        logger.debug(f"{raw_byte_data=}")
        raw_id_bytes = raw_byte_data[:IntColumn.SIZE_IN_BYTES]
        raw_name_bytes = raw_byte_data[IntColumn.SIZE_IN_BYTES:]

        id_instance = IntColumn.deserialize(raw_byte_data=raw_id_bytes)
        name_instance = StrColumn.deserialize(raw_byte_data=raw_name_bytes)
        return cls(id=id_instance, name=name_instance)

