
from typing import List, Tuple
from vendor.companies.providers.filesystem.archive_content_loader import ArchiveContentLoader

from vendor.companies.providers.filesystem.generic_handler import GenericHandler


class ArchiveHandler(GenericHandler):

    def test(self, filename: str) -> bool:
        return True

    def enumerate(self, filename: str) -> List[Tuple[str, str]]:
        return []
    
    def file_loader(self, archive_filename: str, filename: str) -> ArchiveContentLoader:
        pass
