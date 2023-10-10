import logging
import os
from enum import Enum
from typing import Optional, Tuple, cast

from src.config import DATABASE_FILE_NAME
from src.data_types import US2Int

logger = logging.getLogger(__name__)


class Page:
    # First page will be reserved for saving metadata. At this point it will just be saving stuff for out hardcoded Student
    # table. Like row count and root_row address for the Student Table. Later it will save system tables
    PAGE_SIZE = 4096  # 4Kb
    ROWS_PER_PAGE = 89

    # NOTE: Right now due to page size being 4096 bytes and a row being 46 bytes, one page will only have 89 rows
    # which leaves a bit of leftover space at the end. We implement this to make our design a bit easy to code.
    class StateChoice(Enum):
        """
        Right now our page will simply oscillate between clean and dirty state, later we will have multiple
        other state like WRITABLE, NEED_SYNC, etc. when we implement WAL protocol.
        We will be using state that sqlite3 also uses. It can be found in the PgHdr struct, the member is called
        `flags`.

        """

        CLEAN = 0  # Page content either not modified or are flushed to disk
        DIRTY = 0  # Page content modified and need a flush to disk for persistence

    def __init__(self, p_no: US2Int, data: Optional[bytes] = None):
        self.p_no = p_no  # page number
        self.data = cast(bytes, data)

        self.state = self.StateChoice.CLEAN  # When we initialize a page into cache, it's bound to be in a CLEAN state


class Pager:
    TABLE_MAX_PAGES = 1000

    def _open_db_file(self):
        if not os.path.exists(self.db_file_name):
            # Create the file if it doesn't exist
            with open(self.db_file_name, "wb"):
                pass
        return open(self.db_file_name, "r+b")

    def __init__(self):
        self.db_file_name = DATABASE_FILE_NAME
        self.fd = self._open_db_file()
        self.pages: list[Page] = []
        self.dirty_pages: set[Page] = set()
        self.open()

    def open(self):
        if self.fd.closed:
            self.fd = self._open_db_file()
        # Right now, this pages variable is the page cache where we can hold the entirety of database in memory on
        # demand. But obviously which is not going to be the case later, we will have a fix amount of cache size and
        # will need some kinda eviction process.
        self.pages: list[Page] = [Page(p_no=US2Int(idx)) for idx, _ in enumerate(range(self.TABLE_MAX_PAGES))]

    def close(self):
        self.fd.close()
        self.pages = []
        self.dirty_pages = set()

    @property
    def meta_page(self) -> Page:
        return self.get_page(page_num=US2Int(0))

    def mark_page_dirty(self, page_num: US2Int):
        page = self.get_page(page_num=page_num)
        page.state = Page.StateChoice.DIRTY
        self.dirty_pages.add(page)

    def get_page(self, page_num: US2Int) -> Page:
        assert page_num < self.TABLE_MAX_PAGES, "Tried fetching page out of bound"
        if self.pages[page_num].data is None:
            # cache miss, we fetch the page from disk
            page_offset = page_num * Page.PAGE_SIZE
            self.fd.seek(page_offset)
            self.pages[page_num] = Page(p_no=page_num, data=self.fd.read(Page.PAGE_SIZE))
        return self.pages[page_num]

    @staticmethod
    def get_pager_location_from_offset(offset: int) -> Tuple[US2Int, US2Int]:
        # returns the page number and page offset, for a give database file offset
        return US2Int(int(offset / Page.PAGE_SIZE)), US2Int(offset % Page.PAGE_SIZE)

    def page_write(self, offset: int, bytes_to_write: bytes):
        page_num, page_offset = self.get_pager_location_from_offset(offset=offset)
        length_of_write_payload = len(bytes_to_write)
        assert length_of_write_payload < Page.PAGE_SIZE, "Page write not allowed as it exceeds the page size"
        page = self.get_page(page_num=page_num)

        page.data = page.data[:page_offset] + bytes_to_write + page.data[page_offset + length_of_write_payload :]
        page.state = Page.StateChoice.DIRTY
        self.dirty_pages.add(page)
        return page.p_no

    def page_flush(self, page_num: US2Int):
        # Flushes a specific page to disk
        page = self.pages[page_num]
        if page.data is None or page.state == Page.StateChoice.CLEAN:
            logger.warning(f"Skipping flush of unused page, state: {page.state}")
            return
        page_offset = page_num * page.PAGE_SIZE
        if self.fd.tell() != page_offset:
            self.fd.seek(page_offset)
        self.fd.write(page.data)
        self.fd.flush()
        # The normal flush doesn't guarantee data has been flushed to disk, we need a further os.fsync to sync
        # previously flushed data which might have been in the os buffers, to disk to guarantee durability
        os.fsync(self.fd)
        page.state = Page.StateChoice.CLEAN
        self.dirty_pages.remove(page)

    def pager_flush(self):
        # Flushes all dirty pages to disk
        # logger.info(f"Flushing {len(self.dirty_pages)} dirty pages")
        for page in self.dirty_pages:
            page_offset = page.p_no * page.PAGE_SIZE
            if self.fd.tell() != page_offset:
                self.fd.seek(page_offset)
            self.fd.write(page.data)
        self.fd.flush()
        os.fsync(self.fd)
        for page in self.dirty_pages:
            page.state = Page.StateChoice.CLEAN
        self.dirty_pages = set()


pager = Pager()
