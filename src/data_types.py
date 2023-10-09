"""
    Contains classes that inherit native data types and put some helper method to them for ease of use during
    io operations.
"""
import struct
from abc import ABC, abstractmethod
from typing import Any, Union, cast


class BaseDataType(ABC):
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


class S4Int(int, BaseDataType):
    """
    4 byte signed integer
    """

    SIZE_IN_BYTES = 4

    def __new__(cls, value):
        return super().__new__(cls, value)

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(cast(int, self.val))  # the cast is used for static type checker to infer type
        # 2**31 - 1 is the max number that can be represented by a 4 byte signed int
        assert -(2**31) < self.val < 2**31, f"value should be less than {2 ** 31}"
        return self.val

    def serialize(self):
        return struct.pack("i", self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        # logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(struct.unpack("i", raw_byte_data)[0])


class S2Int(S4Int):
    SIZE_IN_BYTES = 2
    """
           4 byte signed integer
    """

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(cast(int, self.val))  # the cast is used for static type checker to infer type
        # 2**15 - 1 is the max number that can be represented by a 2 byte signed int
        assert -(2**15) < self.val < 2**15, f"value should be less than {2 ** 15}"
        return self.val

    def serialize(self):
        return struct.pack("H", self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        # logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(struct.unpack("H", raw_byte_data)[0])


class US4Int(BaseDataType, int):
    """
    4 byte unsigned integer
    """

    SIZE_IN_BYTES = 4

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(cast(int, self.val))  # the cast is used for static type checker to infer type
        # 2**32 - 1 is the max number that can be represented by a 4 byte signed int
        assert 0 <= self.val < 2**32, f"value should be less than {2 ** 32}"
        return self.val

    def serialize(self):
        return struct.pack("i", self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        # logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(struct.unpack("i", raw_byte_data)[0])


class US2Int(S4Int):
    SIZE_IN_BYTES = 2
    """
           2 byte unsigned integer
    """

    def __new__(cls, value):
        return super().__new__(cls, value)

    def validate_val(self) -> int:
        if isinstance(self.val, str):
            assert self.val.isnumeric(), "Int value expected"
        else:
            assert isinstance(self.val, int), "Int value expected"
        self.val = int(cast(int, self.val))  # the cast is used for static type checker to infer type
        # 2**62 - 1 is the max number that can be represented by a 2 byte signed int
        assert 0 <= self.val < 2**16, f"value should be less than {2 ** 16}"
        return self.val

    def serialize(self):
        return struct.pack("H", self.val)

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        # logger.debug(f"int deserialize {raw_byte_data=}")

        return cls(struct.unpack("H", raw_byte_data)[0])


class Str(BaseDataType):
    """

    Right now str columne will just have 4 bytes space
    """

    SIZE_IN_BYTES = 32

    def validate_val(self) -> str:
        assert isinstance(self.val, str), "str value expected"
        assert len(self.val) < self.SIZE_IN_BYTES, "Max 32 characters allowed"

        return self.val

    def serialize(self):
        return struct.pack(f"{self.SIZE_IN_BYTES}s", self.val.ljust(32, "\0").encode("UTF-8"))

    @classmethod
    def deserialize(cls, raw_byte_data: bytes):
        # logger.debug(f"str deserialize {raw_byte_data=}")
        unpacked_data = struct.unpack(f"{cls.SIZE_IN_BYTES}s", raw_byte_data)[0]

        return cls(val=unpacked_data.decode("UTF-8").rstrip("\0"))
