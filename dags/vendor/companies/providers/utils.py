
from os import listdir, path
from typing import Generator, Tuple


def enumerate_files(data_path: str) -> Generator[Tuple[str, str], None, None]:
    for fpath in listdir(data_path):
        file_path = path.join(data_path, fpath)
        if path.isfile(file_path):
            _, ext = path.splitext(fpath)
            yield (file_path, ext)
        else:
            for file in enumerate_files(file_path):
                yield file
