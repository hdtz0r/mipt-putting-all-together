from orjson import loads
from typing import Dict, List
from vendor.companies.providers.datasource import Datasource
from vendor.companies.providers.content_loader import ContentLoader
from vendor.companies.providers.filesystem.archive_content_loader import ArchiveContentLoader
from vendor.companies.providers.filesystem.file_handler import FileHandler


class JsonContentLoader(ContentLoader):

    _filename: str = None
    _archive_file_loader: ArchiveContentLoader = None

    def __init__(self, filename: str, archive_file_loader: ArchiveContentLoader) -> None:
        self._filename = filename
        self._archive_file_loader = archive_file_loader

    def load(self) -> List[Dict[str, any]]:
        json = None
        if self._archive_file_loader:
            json = loads(self._archive_file_loader.load())
        else:
            with open(self._filename, "rb", buffering=0) as file:
                json = loads(file.read())

        if isinstance(json, list):
            return json
        else:
            return [json]


class JsonFileHandler(FileHandler):

    def load(self, filename: str, archive_file_loader: ArchiveContentLoader) -> List[Datasource]:
        return [Datasource(filename, JsonContentLoader(filename, archive_file_loader))]

    def get_ext(self) -> List[str]:
        return ["json"]
