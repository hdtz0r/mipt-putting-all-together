from os import path
from typing import IO, Dict, List, Tuple
from zipfile import ZipFile, is_zipfile
from vendor.companies.providers.filesystem.archive_content_loader import ArchiveContentLoader

from vendor.companies.providers.filesystem.archive_handler import ArchiveHandler


class ZipFileContentLoader(ArchiveContentLoader):

    _filename: str = None
    _archive_filename: str = None
    _archives: Dict[str, ZipFile] = {}

    def __init__(self, archive_filename: str, filename: str) -> None:
        self._filename = filename
        self._archive_filename = archive_filename

    def load(self) -> IO[bytes]:
        zip_archive = ZipFileContentLoader._archives.get(self._archive_filename, None)
        if not zip_archive:
            zip_archive = ZipFile(self._archive_filename, mode="r")
            ZipFileContentLoader._archives[self._archive_filename] = zip_archive
        else:
            pass # TODO handle nested archives

        with zip_archive.open(self._filename, "r", force_zip64=True) as file:
            return file.read()
        
    @staticmethod
    def release():
        for archive in ZipFileContentLoader._archives.values():
            archive.close()


class ZipFileHandler(ArchiveHandler):

    def test(self, filename: str) -> bool:
        return is_zipfile(filename)

    def enumerate(self, filename: str) -> List[Tuple[str, str,]]:
        files = []
        if is_zipfile(filename):
            try:
                zip_archive = ZipFile(filename, mode="r")
                for name, ext in self._enumerate_files_in_archive(filename, zip_archive):
                    files.append((name, ext))
            except Exception as ex:
                self.warn(
                    f"Could not extract files from archive {filename} cause {ex}")
        else:
            self.warn(f"The file {filename} is not valid zip archive")
        return files

    def _enumerate_files_in_archive(self, filename: str, zip_archive: ZipFile):
        try:
            for file in zip_archive.infolist():
                if not file.is_dir():
                    _, ext = path.splitext(file.filename)
                    if ext[1:] in self.get_ext():
                        with zip_archive.open(file, "r") as buffer:
                            try:
                                for nested_file_name, nested_file_ext in self._enumerate_files_in_archive(filename, ZipFile(buffer, "r")):
                                    yield (nested_file_name, nested_file_ext)
                            except Exception as ex:
                                self.warn(
                                    f"Could not extract files from archive {file.filename} cause {ex}")
                    else:
                        yield (file.filename, ext[1:])
        finally:
            zip_archive.close()

    def get_ext(self) -> List[str]:
        return ["zip"]

    def file_loader(self, archive_filename: str, filename: str) -> ArchiveContentLoader:
        return ZipFileContentLoader(archive_filename, filename)
    
    def cleanup(self):
        ZipFileContentLoader.release()
        return super().cleanup()
