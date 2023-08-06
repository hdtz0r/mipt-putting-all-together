
from typing import Type


def import_class(package: str, name: str) -> Type:
    if not package or not name:
        raise ValueError("Package or class name can not be null")

    canonical_name_characters = []
    pos = 0
    for char in name:
        if ord(char) < 96:
            if pos > 1:
                canonical_name_characters.append("_")
            canonical_name_characters.append(char.lower())
        else:
            canonical_name_characters.append(char)
        pos += 1

    module_name = f"{package}.{''.join(canonical_name_characters)}"

    class_ctor = None
    module = __import__(module_name, fromlist=[name])
    class_ctor = getattr(module, name)
    return class_ctor
