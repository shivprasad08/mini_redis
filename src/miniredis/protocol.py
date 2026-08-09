"""
Custom RESP-like wire protocol for mini-redis.

This module implements encoding and decoding for a text-based,
Redis-inspired protocol used on the client-facing TCP surface.
It is hand-rolled from scratch — no external protocol libraries.

Wire format summary
───────────────────
Every message starts with a single type byte, followed by type-specific
payload, and terminated by ``\\r\\n``.

  Type byte   Meaning            Example
  ─────────   ───────            ───────
  +           Simple string      +OK\\r\\n
  -           Error              -ERR unknown command\\r\\n
  :           Integer            :42\\r\\n
  $           Bulk string        $3\\r\\nfoo\\r\\n
              Null bulk string   $-1\\r\\n
  *           Array              *2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n
              Null array         *-1\\r\\n

Client commands are sent as arrays of bulk strings:

    SET foo bar  →  *3\\r\\n$3\\r\\nSET\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n

Bulk strings are binary-safe: the explicit length prefix means the
payload may contain arbitrary bytes, including ``\r\n``.
"""

from __future__ import annotations

import dataclasses
from typing import List, Optional, Union


# ──────────────────────────────────────────────────────────────────────
# Exceptions
# ──────────────────────────────────────────────────────────────────────

class ProtocolError(Exception):
    """Raised when the incoming byte stream is genuinely malformed.

    This is *not* raised for merely incomplete data — that situation is
    signalled by returning ``None`` from the parser instead.
    """

class ErrorReply(Exception):
    """Represents a parsed RESP error (-ERR) from the server.
    Used by the client to distinguish errors from simple strings.
    """
    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)



# ──────────────────────────────────────────────────────────────────────
# Command container
# ──────────────────────────────────────────────────────────────────────

@dataclasses.dataclass(frozen=True, slots=True)
class Command:
    """A fully parsed client command.

    Attributes:
        name: The command verb, always uppercased (e.g. ``"SET"``).
        args: The remaining arguments as raw ``bytes`` values.

    *Why bytes?*  Bulk strings are binary-safe — a value can legally
    contain arbitrary bytes (including ``\\r\\n``).  Keeping args as
    ``bytes`` preserves this guarantee and avoids lossy decoding.
    The command *name* is decoded to ``str`` for ergonomic dispatch.
    """

    name: str
    args: list[bytes]


# ──────────────────────────────────────────────────────────────────────
# Constants
# ──────────────────────────────────────────────────────────────────────

CRLF: bytes = b"\r\n"
CRLF_LEN: int = len(CRLF)


# ──────────────────────────────────────────────────────────────────────
# Encoding helpers
# ──────────────────────────────────────────────────────────────────────

def encode_simple_string(value: str) -> bytes:
    """Encode a simple string response.

    >>> encode_simple_string("OK")
    b'+OK\\r\\n'
    """
    return b"+" + value.encode() + CRLF


def encode_error(message: str) -> bytes:
    """Encode an error response.

    The message should already include the error type prefix
    (e.g. ``"ERR unknown command"``).  If it does not start with a
    known error type, ``"ERR "`` is prepended automatically.

    >>> encode_error("ERR unknown command 'OOPS'")
    b"-ERR unknown command 'OOPS'\\r\\n"
    """
    return b"-" + message.encode() + CRLF


def encode_integer(value: int) -> bytes:
    """Encode an integer response.

    >>> encode_integer(42)
    b':42\\r\\n'
    """
    return b":" + str(value).encode() + CRLF


def encode_bulk_string(value: Optional[bytes]) -> bytes:
    """Encode a bulk string response (binary-safe).

    Pass ``None`` to encode the null bulk string sentinel (``$-1\\r\\n``).

    >>> encode_bulk_string(b"hello")
    b'$5\\r\\nhello\\r\\n'
    >>> encode_bulk_string(None)
    b'$-1\\r\\n'
    """
    if value is None:
        return b"$-1" + CRLF
    return b"$" + str(len(value)).encode() + CRLF + value + CRLF


def encode_array(items: Optional[List[bytes]]) -> bytes:
    """Encode an array response.

    Each element in *items* must already be a fully encoded wire-format
    message (e.g. the output of one of the other ``encode_*`` helpers).
    Pass ``None`` to encode the null array sentinel (``*-1\\r\\n``).

    >>> encode_array([encode_bulk_string(b"foo"), encode_bulk_string(b"bar")])
    b'*2\\r\\n$3\\r\\nfoo\\r\\n$3\\r\\nbar\\r\\n'
    >>> encode_array(None)
    b'*-1\\r\\n'
    """
    if items is None:
        return b"*-1" + CRLF
    header = b"*" + str(len(items)).encode() + CRLF
    return header + b"".join(items)


# ──────────────────────────────────────────────────────────────────────
# Streaming decoder / parser
# ──────────────────────────────────────────────────────────────────────

class CommandParser:
    """Incremental, non-blocking parser for client commands.

    Usage::

        parser = CommandParser()
        parser.feed(chunk_from_network)
        cmd = parser.try_parse()   # Command | None

    The parser maintains an internal byte buffer so that TCP
    fragmentation is handled transparently.  ``try_parse()`` never
    blocks; it returns ``None`` when more data is needed and raises
    ``ProtocolError`` only on genuinely malformed input.
    """

    def __init__(self) -> None:
        self._buffer: bytearray = bytearray()

    # ── public API ──────────────────────────────────────────────────

    def feed(self, data: bytes) -> None:
        """Append *data* received from the network to the internal buffer."""
        self._buffer.extend(data)

    def try_parse(self) -> Optional[Command]:
        """Try to extract one complete command from the buffer.

        Returns a :class:`Command` if a full array-of-bulk-strings
        command is available, otherwise ``None``.

        Raises :class:`ProtocolError` on malformed data.
        """
        result = self._try_parse_element()
        if result is None:
            return None

        consumed, elements = result

        # A client command must be an array of bulk strings.
        if not isinstance(elements, list):
            raise ProtocolError(
                "Expected an array (*) at the top level of a client command, "
                f"got type byte {chr(self._buffer[0])!r}"
            )

        if len(elements) == 0:
            # Consume the empty array and return None (no command).
            self._consume(consumed)
            return None

        # Each element must be a bulk-string bytes value.
        for i, elem in enumerate(elements):
            if not isinstance(elem, bytes):
                raise ProtocolError(
                    f"Command argument {i} is not a bulk string"
                )

        self._consume(consumed)

        name = elements[0].decode("utf-8", errors="replace").upper()
        args = list(elements[1:])
        return Command(name=name, args=args)

    _INCOMPLETE = object()  # sentinel distinct from a legitimate None (nil) response

    def try_parse_response(self) -> Union[bytes, list, int, str, None, object]:
        """Try to extract one complete RESP response from the buffer.
        Used by the client to read server replies.

        Returns ``CommandParser._INCOMPLETE`` when more data is needed —
        this is distinct from a legitimate parsed value of ``None``,
        which represents an actual RESP null bulk/array response.
        """
        result = self._try_parse_element()
        if result is None:
            return self._INCOMPLETE
        consumed, value = result
        self._consume(consumed)
        return value

    # ── internal parsing machinery ──────────────────────────────────

    def _consume(self, n: int) -> None:
        """Remove the first *n* bytes from the buffer."""
        del self._buffer[:n]

    def _find_crlf(self, start: int = 0) -> Optional[int]:
        """Return the index of the next ``\\r\\n`` at or after *start*,
        or ``None`` if not found."""
        idx = self._buffer.find(CRLF, start)
        return idx if idx != -1 else None

    def _try_parse_element(self, offset: int = 0) -> Optional[tuple[int, Union[bytes, list, int, str]]]:
        """Parse one RESP element starting at *offset* inside the buffer.

        Returns ``(bytes_consumed_from_offset, parsed_value)`` or
        ``None`` if the data is incomplete.

        Raises :class:`ProtocolError` on genuinely invalid framing.
        """
        if offset >= len(self._buffer):
            return None  # need more data

        type_byte = self._buffer[offset]
        type_char = chr(type_byte)

        if type_char == "$":
            return self._parse_bulk_string(offset)
        elif type_char == "*":
            return self._parse_array(offset)
        elif type_char == "+":
            return self._parse_simple_string(offset)
        elif type_char == "-":
            return self._parse_error_string(offset)
        elif type_char == ":":
            return self._parse_integer(offset)
        else:
            raise ProtocolError(
                f"Unknown type byte {type_char!r} (0x{type_byte:02x}) "
                f"at buffer position {offset}"
            )

    def _read_line(self, offset: int) -> Optional[tuple[int, bytes]]:
        """Read bytes from *offset* up to the next ``\\r\\n``.

        Returns ``(total_bytes_consumed, line_content)`` or ``None``
        if no complete line is available yet.
        """
        crlf_pos = self._find_crlf(offset)
        if crlf_pos is None:
            return None
        line = bytes(self._buffer[offset:crlf_pos])
        consumed = crlf_pos - offset + CRLF_LEN
        return consumed, line

    def _parse_length(self, line: bytes, type_char: str) -> int:
        """Parse an integer length/count from a header line (after the
        type byte).  Raises ``ProtocolError`` on non-numeric content."""
        try:
            return int(line)
        except ValueError:
            raise ProtocolError(
                f"Non-numeric length in {type_char} header: {line!r}"
            )

    # ── type-specific parsers ───────────────────────────────────────

    def _parse_simple_string(self, offset: int) -> Optional[tuple[int, str]]:
        result = self._read_line(offset + 1)  # skip '+'
        if result is None:
            return None
        consumed, line = result
        return consumed + 1, line.decode("utf-8", errors="replace")

    def _parse_error_string(self, offset: int) -> Optional[tuple[int, ErrorReply]]:
        result = self._read_line(offset + 1)  # skip '-'
        if result is None:
            return None
        consumed, line = result
        return consumed + 1, ErrorReply(line.decode("utf-8", errors="replace"))

    def _parse_integer(self, offset: int) -> Optional[tuple[int, int]]:
        result = self._read_line(offset + 1)  # skip ':'
        if result is None:
            return None
        consumed, line = result
        try:
            value = int(line)
        except ValueError:
            raise ProtocolError(
                f"Non-numeric integer value: {line!r}"
            )
        return consumed + 1, value

    def _parse_bulk_string(self, offset: int) -> Optional[tuple[int, Optional[bytes]]]:
        # Read the "$<length>\r\n" header line.
        header_result = self._read_line(offset + 1)  # skip '$'
        if header_result is None:
            return None
        header_consumed, length_bytes = header_result
        length = self._parse_length(length_bytes, "$")

        if length == -1:
            # Null bulk string sentinel.
            return header_consumed + 1, None

        if length < -1:
            raise ProtocolError(
                f"Invalid bulk string length: {length}"
            )

        # We need <length> data bytes + \r\n after the header.
        data_start = offset + 1 + header_consumed
        data_end = data_start + length
        total_end = data_end + CRLF_LEN

        if total_end > len(self._buffer):
            return None  # incomplete, need more data

        # Validate trailing CRLF.
        if self._buffer[data_end:total_end] != CRLF:
            raise ProtocolError(
                "Bulk string not terminated by \\r\\n"
            )

        payload = bytes(self._buffer[data_start:data_end])
        total_consumed = total_end - offset
        return total_consumed, payload

    def _parse_array(self, offset: int) -> Optional[tuple[int, Optional[list]]]:
        # Read the "*<count>\r\n" header line.
        header_result = self._read_line(offset + 1)  # skip '*'
        if header_result is None:
            return None
        header_consumed, count_bytes = header_result
        count = self._parse_length(count_bytes, "*")

        if count == -1:
            # Null array sentinel.
            return header_consumed + 1, None

        if count < -1:
            raise ProtocolError(
                f"Invalid array count: {count}"
            )

        # Parse <count> child elements sequentially.
        elements: list = []
        child_offset = offset + 1 + header_consumed
        for _ in range(count):
            child_result = self._try_parse_element(child_offset)
            if child_result is None:
                return None  # incomplete
            child_consumed, child_value = child_result
            elements.append(child_value)
            child_offset += child_consumed

        total_consumed = child_offset - offset
        return total_consumed, elements
