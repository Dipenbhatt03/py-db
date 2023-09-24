import logging
import os
import re
from enum import Enum
from time import time

import config  # noqa
from row import Row, IntColumn, StrColumn
from table import student_table

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)


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
        flush_to_disk()
        exit(0)
    return MetaCommandResult.COMMAND_NOT_RECOGNIZED


def timing(fun, *args, **kwargs):
    def wrapper(*args, **kwargs):
        t = time()
        fun(*args, **kwargs)
        logger.info(f"Executed in {time() - t} seconds")

    return wrapper


def flush_to_disk(row_raw_data):
    with open("dipen.db", "ab") as file:
        file.write(row_raw_data)


def execute_insert(row: Row):
    raw_data = row.serialize()
    logger.debug(f"raw data to insert = {raw_data}")
    student_table.rows += 1
    flush_to_disk(row_raw_data=raw_data)


@timing
def execute_read(name_instance: StrColumn = None, id_instance: IntColumn = None):
    logger.info(f"{name_instance=} {id_instance=}")
    matched_rows = 0
    t = time()

    for r_id in range(student_table.rows):
        row_instance = Row.fetch_row(row_number=r_id)
        if name_instance and row_instance.name.val != name_instance.val:
            continue
        if id_instance and row_instance.id.val != id_instance.val:
            continue
        logger.info(f"{row_instance}")
        matched_rows += 1
    logger.info(f"Total {matched_rows} matched rows out of {student_table.rows} rows read in {time() - t} seconds")


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
    if os.path.exists("dipen.db"):
        with open("dipen.db", "rb") as file:
            file.seek(0, os.SEEK_END)
            size = file.tell()
            student_table.rows = int(size / Row.size())

    while True:
        input_buffer = input(">")
        input_buffer = re.sub(r'\s+', ' ', input_buffer.strip())
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
