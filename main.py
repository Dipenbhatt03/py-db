import logging
import re
from enum import Enum
from time import time
from typing import Optional

import config  # noqa
from row import IntColumn, Row, StrColumn
from table import student_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
import sys

sys.setrecursionlimit(100001)


class MetaCommandResult(Enum):
    SUCCESS = 0
    COMMAND_NOT_RECOGNIZED = 1


class StatementType(Enum):
    SELECT = 0
    INSERT = 1


class Statement:
    def __init__(self, query: str, statement_type: StatementType):
        self.query = query
        self.statement_type = statement_type


class PrepareStatementResult(Enum):
    SUCCESS = 0
    SYNTAX_ERROR = 1
    STATEMENT_NOT_RECOGNIZED = 2


def do_meta_command(input_buffer):
    if input_buffer == ".exit":
        exit(0)
    return MetaCommandResult.COMMAND_NOT_RECOGNIZED


def timing(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        t = time()
        fun(*args, **kwargs)
        logger.info(f"Executed in {time() - t} seconds")

    return wrapper


def execute_insert(row: Row):
    raw_data = row.serialize()
    logger.debug(f"raw data to insert = {raw_data}")
    student_table.insert(row=row)


@timing
def execute_read(name_instance: Optional[StrColumn] = None, id_instance: Optional[IntColumn] = None):
    logger.info(f"{name_instance=} {id_instance=}")
    rows = student_table.binary_tree.traverse(
        root_row=student_table.root_row, row_id_to_search=id_instance.val if id_instance else None
    )
    for row in rows:
        logger.info(f"{row=}")

    logger.info(f"Total {len(rows)} matched rows out of {student_table.row_count} rows read")


"""
select clause regex
select\s+((name\s*=\s*(".*"|'.*'))|(id\s*=\s*\d+)|(^))
"""


def execute_statement(statement: Statement):
    if statement.statement_type == StatementType.INSERT:
        # we match the pattern `insert into students values ({id}, {name})`
        pattern = r"insert into students values \((\d+), '([^']+)'\)"
        match = re.search(pattern, statement.query)
        if not match:
            logger.error(f"Invalid query {statement.query}")
            return PrepareStatementResult.SYNTAX_ERROR
        id = match.group(1)
        name = match.group(2)
        row = Row(id=IntColumn(val=id), name=StrColumn(val=name))

        execute_insert(row=row)
        # try:
        #
        # except AssertionError as e:
        #     logger.error(e)
        #     return PrepareStatementResult.SYNTAX_ERROR
        return PrepareStatementResult.SUCCESS
        # flush_to_disk()
    else:
        pattern = r'select(?:\s+name\s*=\s*["\']([^"\']*)["\'])?(?:\s+id\s*=\s*(\d+))?'
        match = re.search(pattern, statement.query)
        if not match:
            logger.error(f"Invalid query {statement.query}")
            return PrepareStatementResult.SYNTAX_ERROR
        name_instance = None
        id_instance = None

        try:
            if match.group(1):
                name_instance = StrColumn(match.group(1))
            if match.group(2):
                id_instance = IntColumn(match.group(2))
        except AssertionError as e:
            logger.error(e)
            return PrepareStatementResult.SYNTAX_ERROR

        execute_read(name_instance=name_instance, id_instance=id_instance)


def prepare_statement(input_buffer: str) -> PrepareStatementResult:
    match input_buffer.split(" ")[0].strip().lower():
        case "insert":
            # logger.info("This is where we insert")
            return execute_statement(Statement(query=input_buffer, statement_type=StatementType.INSERT))
        case "select":
            # logger.info("This is where we print all rows")
            return execute_statement(Statement(query=input_buffer, statement_type=StatementType.SELECT))
        case _:
            return PrepareStatementResult.STATEMENT_NOT_RECOGNIZED


def main():
    # We implement a basic REPL (Read Execute Print Loop) loop

    # on program start, we check if there is a existing file on disk and load it if present
    # if os.path.exists(DATABASE_FILE_NAME):
    #     with open(DATABASE_FILE_NAME, "rb") as file:
    #         raw_data = file.read()
    #         if raw_data:
    #             # checksum, raw_table_data = raw_data[:32], raw_data[32:]
    #             # assert hashlib.sha256(raw_table_data).digest() == checksum
    #
    #             student_table.rows = int(len(raw_data) / Row.size())
    #             student_table.raw_data = raw_data

    while True:
        input_buffer = input(">")
        input_buffer = re.sub(r"\s+", " ", input_buffer.strip())
        if input_buffer.startswith("."):
            if do_meta_command(input_buffer) == MetaCommandResult.COMMAND_NOT_RECOGNIZED:
                logger.error(f"Command {input_buffer} not recognized")
            continue
        try:
            match prepare_statement(input_buffer):
                case PrepareStatementResult.STATEMENT_NOT_RECOGNIZED:
                    logger.error(f"Statement {input_buffer} not recognized")
                case PrepareStatementResult.SYNTAX_ERROR:
                    logger.error(f"Syntax error")
                case _:
                    continue
        except AssertionError as e:
            logger.error(e)
            continue


if __name__ == "__main__":
    main()
