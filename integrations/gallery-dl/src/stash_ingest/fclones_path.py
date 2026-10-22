"""The STFU-8 byte-path encoding used inside fclones' Unix JSON reports.

JSON decoding alone does not decode these paths. This is the decode_u8 grammar
from stfu8 0.2, used by fclones 0.35 (not Python's unicode_escape codec):
https://github.com/pkolaczk/fclones/blob/v0.35.0/fclones/src/path.rs
https://docs.rs/stfu8/0.2.6/stfu8/fn.decode_u8.html
"""

import re

from .encoding import InvalidData

_ESCAPE = re.compile(r"\\(?:[tnr\\]|x[0-9a-fA-F]{2}|u[0-9a-fA-F]{6})|\\")
_SIMPLE = {"t": b"\t", "n": b"\n", "r": b"\r", "\\": b"\\"}


def decode_path(value):
    """Return the literal Unix filename, retaining non-UTF-8 bytes losslessly."""
    if not isinstance(value, str) or len(value) > 32768:
        raise InvalidData("Invalid or oversized fclones path")
    result, offset = bytearray(), 0
    try:
        for match in _ESCAPE.finditer(value):
            result.extend(value[offset:match.start()].encode("utf-8"))
            token = match[0]
            if len(token) == 1:
                raise InvalidData("Invalid fclones STFU-8 path escape")
            if token[1] in _SIMPLE:
                result.extend(_SIMPLE[token[1]])
            elif token[1] == "x":
                result.append(int(token[2:], 16))
            else:
                result.extend(chr(int(token[2:], 16)).encode("utf-8"))
            offset = match.end()
        result.extend(value[offset:].encode("utf-8"))
    except (UnicodeError, ValueError):
        raise InvalidData("Invalid fclones STFU-8 path encoding") from None
    return result.decode("utf-8", "surrogateescape")
