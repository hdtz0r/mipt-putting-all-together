from os import listdir, path
from typing import IO, Callable, Dict
from vendor.config.configuration import Configuration
from vendor.companies.providers.data_provider import DataProvider
from vendor.companies.providers.filesystem.archive_content_loader import ArchiveContentLoader
from vendor.companies.providers.filesystem.archive_handler import ArchiveHandler
from vendor.companies.providers.filesystem.file_handler import FileHandler
from vendor.companies.providers.filesystem.generic_handler import GenericHandler
from vendor.companies.providers.utils import enumerate_files


class FileDataProvider(DataProvider):

    _archive_handlers: Dict[str, ArchiveHandler] = None
    _file_handlers: Dict[str, FileHandler] = None
    _data_path: str

    def __init__(self, configuration: Configuration) -> None:
        super().__init__(configuration)
        self._file_handlers = dict()
        self._archive_handlers = dict()
        self._register_handlers()
        self._data_path = configuration.property('data-path')

    def initialize(self):
        if self._data_path:
            self._enumerate_and_register_files(self._data_path)

    def _register_handlers(self):
        self._register_handlers_internal(
            "files", lambda ext, file_handler: self._register_file_handler(ext, file_handler))
        self._register_handlers_internal(
            "archives", lambda ext, archive_handler: self._register_archive_handler(ext, archive_handler))

    def _register_file_handler(self, ext: str, handler: FileHandler):
        self._file_handlers[ext] = handler

    def _register_archive_handler(self, ext: str, handler: ArchiveHandler):
        self._archive_handlers[ext] = handler

    def _register_handlers_internal(self, package: str, module_consumer: Callable[[GenericHandler], None]):
        handler_modules_path = path.join(
            path.dirname(__file__), "filesystem", package)
        for module_file in filter(lambda file_name: not file_name.startswith("_"), listdir(handler_modules_path)):
            module_name = path.splitext(module_file)[0]
            try:
                module_path = f"vendor.companies.providers.filesystem.{package}.{module_name}"
                module = __import__(module_path, fromlist=[module_name])
                for attr in dir(module):
                    handler_ctor = getattr(module, attr)
                    if isinstance(handler_ctor, (type, )) and issubclass(handler_ctor, GenericHandler):
                        handler = handler_ctor()
                        for ext in handler.get_ext():
                            module_consumer(ext, handler)
                            self.debug(
                                f"Registred file handler {handler_ctor.__name__} from module {module_name} for ext {ext}")
            except Exception as ex:
                self.warn(
                    f"Could not load handler from module {module_name}, cause {ex}")

    def _enumerate_and_register_files(self, data_path: str):
        for file, ext in enumerate_files(data_path):
            dotless_ext = ext[1:]
            if dotless_ext:
                if dotless_ext in self._archive_handlers:
                    archive_handler = self._archive_handlers.get(dotless_ext)
                    if archive_handler.test(file):
                        for archive_file, archive_file_ext in archive_handler.enumerate(file):
                            loader = archive_handler.file_loader(
                                file, archive_file)
                            self._handle_file(
                                archive_file, archive_file_ext, loader)
                    else:
                        self.warn(
                            f"Could not process an archive {file} since its signature is illegal or file was corrupted")
                else:
                    file_buffer = None
                    try:
                        file_buffer: IO[bytes] = open(
                            file, mode="r", encoding="utf8")
                        self._handle_file(file, dotless_ext, file_buffer)
                    except Exception as ex:
                        if file_buffer:
                            file_buffer.close()
                        self.warn(f"Could not open file {file} cause {ex}")

    def _handle_file(self, file: str, ext: str, loader: ArchiveContentLoader = None):
        file_handler = self._file_handlers.get(ext, None)
        if file_handler:
            for datasource in file_handler.load(file, loader):
                self.debug(
                    f"Datasource {file} is initialized using {file_handler.__class__.__name__}")
                self.add_datasource(datasource)
        else:
            self.warn(f"There is no file handler for extension {ext}")

    def cleanup(self):
        for archive_handler in self._archive_handlers.values():
            try:
                archive_handler.cleanup()
            except Exception as ex:
                self.error(
                    "Could not cleanup resources used by the handler", ex)

        for file_handler in self._file_handlers.values():
            try:
                file_handler.cleanup()
            except Exception as ex:
                self.error(
                    "Could not cleanup resources used by the handler", ex)

        return super().cleanup()
