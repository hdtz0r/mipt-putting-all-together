
from typing import List
from vendor.companies.providers.datasource import Datasource
from vendor.companies.providers.filesystem.archive_content_loader import ArchiveContentLoader
from vendor.companies.providers.filesystem.generic_handler import GenericHandler


class FileHandler(GenericHandler):

    def load(self, filename: str, archive_file_loader: ArchiveContentLoader = None) -> List[Datasource]:
        pass
