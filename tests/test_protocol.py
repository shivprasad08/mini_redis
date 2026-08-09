"""
Tests for the custom RESP-like wire protocol.

Covers encoding, streaming/incremental decoding, pipelining,
malformed input, binary safety, and edge cases.
"""

from __future__ import annotations

import pytest

from miniredis.protocol import (
    Command,
    CommandParser,
    ProtocolError,
    encode_array,
    encode_bulk_string,
    encode_error,
    encode_integer,
    encode_simple_string,
)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Encoding tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestEncodeSimpleString:
    def test_ok(self) -> None:
        assert encode_simple_string("OK") == b"+OK\r\n"

    def test_empty(self) -> None:
        assert encode_simple_string("") == b"+\r\n"

    def test_with_spaces(self) -> None:
        assert encode_simple_string("hello world") == b"+hello world\r\n"


class TestEncodeError:
    def test_basic(self) -> None:
        assert encode_error("ERR unknown command") == b"-ERR unknown command\r\n"

    def test_custom_type(self) -> None:
        assert encode_error("WRONGTYPE Operation") == b"-WRONGTYPE Operation\r\n"


class TestEncodeInteger:
    def test_positive(self) -> None:
        assert encode_integer(42) == b":42\r\n"

    def test_zero(self) -> None:
        assert encode_integer(0) == b":0\r\n"

    def test_negative(self) -> None:
        assert encode_integer(-1) == b":-1\r\n"

    def test_large(self) -> None:
        assert encode_integer(999999) == b":999999\r\n"


class TestEncodeBulkString:
    def test_basic(self) -> None:
        assert encode_bulk_string(b"hello") == b"$5\r\nhello\r\n"

    def test_empty(self) -> None:
        assert encode_bulk_string(b"") == b"$0\r\n\r\n"

    def test_null(self) -> None:
        assert encode_bulk_string(None) == b"$-1\r\n"

    def test_binary_with_crlf(self) -> None:
        """Bulk strings must be binary-safe — a payload containing \\r\\n
        must survive a round-trip thanks to the explicit length prefix."""
        data = b"hel\r\nlo"
        encoded = encode_bulk_string(data)
        assert encoded == b"$7\r\nhel\r\nlo\r\n"


class TestEncodeArray:
    def test_basic(self) -> None:
        result = encode_array([
            encode_bulk_string(b"foo"),
            encode_bulk_string(b"bar"),
        ])
        assert result == b"*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"

    def test_empty(self) -> None:
        assert encode_array([]) == b"*0\r\n"

    def test_null(self) -> None:
        assert encode_array(None) == b"*-1\r\n"

    def test_mixed_types(self) -> None:
        result = encode_array([
            encode_simple_string("OK"),
            encode_integer(3),
            encode_bulk_string(b"data"),
        ])
        assert result == b"*3\r\n+OK\r\n:3\r\n$4\r\ndata\r\n"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Round-trip encode → decode tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def _make_command_bytes(*args: bytes) -> bytes:
    """Build a wire-format command (array of bulk strings)."""
    parts = [encode_bulk_string(a) for a in args]
    return encode_array(parts)


class TestRoundTrip:
    """Encode a command with the encoder, then decode it with CommandParser."""

    def test_set_command(self) -> None:
        wire = _make_command_bytes(b"SET", b"mykey", b"myvalue")
        parser = CommandParser()
        parser.feed(wire)
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "SET"
        assert cmd.args == [b"mykey", b"myvalue"]

    def test_get_command(self) -> None:
        wire = _make_command_bytes(b"GET", b"mykey")
        parser = CommandParser()
        parser.feed(wire)
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "GET"
        assert cmd.args == [b"mykey"]

    def test_command_name_uppercased(self) -> None:
        wire = _make_command_bytes(b"set", b"key", b"val")
        parser = CommandParser()
        parser.feed(wire)
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "SET"

    def test_no_args(self) -> None:
        wire = _make_command_bytes(b"PING")
        parser = CommandParser()
        parser.feed(wire)
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "PING"
        assert cmd.args == []


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Streaming / incremental parse tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestStreamingParse:
    """Simulate TCP fragmentation by feeding partial chunks."""

    def test_split_at_3_bytes(self) -> None:
        wire = _make_command_bytes(b"SET", b"foo", b"bar")
        parser = CommandParser()

        parser.feed(wire[:3])
        assert parser.try_parse() is None  # incomplete

        parser.feed(wire[3:])
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "SET"
        assert cmd.args == [b"foo", b"bar"]

    def test_byte_by_byte(self) -> None:
        """Feeding one byte at a time must eventually yield the command."""
        wire = _make_command_bytes(b"GET", b"k")
        parser = CommandParser()

        for i, byte in enumerate(wire):
            parser.feed(bytes([byte]))
            result = parser.try_parse()
            if i < len(wire) - 1:
                assert result is None
            else:
                assert result is not None
                assert result.name == "GET"
                assert result.args == [b"k"]

    def test_split_mid_bulk_payload(self) -> None:
        """Split right in the middle of a bulk string payload."""
        wire = _make_command_bytes(b"SET", b"key", b"longvalue")
        # Find a split point inside the 'longvalue' payload.
        split = wire.index(b"longv") + 3
        parser = CommandParser()

        parser.feed(wire[:split])
        assert parser.try_parse() is None

        parser.feed(wire[split:])
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.args == [b"key", b"longvalue"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Pipelining tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestPipelining:
    """Multiple commands arriving in a single feed() call."""

    def test_two_commands_one_feed(self) -> None:
        wire = _make_command_bytes(b"SET", b"a", b"1")
        wire += _make_command_bytes(b"GET", b"a")

        parser = CommandParser()
        parser.feed(wire)

        cmd1 = parser.try_parse()
        assert cmd1 is not None
        assert cmd1.name == "SET"

        cmd2 = parser.try_parse()
        assert cmd2 is not None
        assert cmd2.name == "GET"

        assert parser.try_parse() is None  # no more

    def test_three_pipelined(self) -> None:
        wire = (
            _make_command_bytes(b"SET", b"x", b"10")
            + _make_command_bytes(b"INCR", b"x")
            + _make_command_bytes(b"GET", b"x")
        )
        parser = CommandParser()
        parser.feed(wire)

        names = []
        while True:
            cmd = parser.try_parse()
            if cmd is None:
                break
            names.append(cmd.name)

        assert names == ["SET", "INCR", "GET"]


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Malformed input tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestMalformedInput:
    def test_unknown_type_byte(self) -> None:
        parser = CommandParser()
        parser.feed(b"!garbage\r\n")
        with pytest.raises(ProtocolError, match="Unknown type byte"):
            parser.try_parse()

    def test_non_numeric_bulk_length(self) -> None:
        parser = CommandParser()
        parser.feed(b"*1\r\n$abc\r\n")
        with pytest.raises(ProtocolError, match="Non-numeric length"):
            parser.try_parse()

    def test_non_numeric_array_count(self) -> None:
        parser = CommandParser()
        parser.feed(b"*xyz\r\n")
        with pytest.raises(ProtocolError, match="Non-numeric length"):
            parser.try_parse()

    def test_negative_bulk_length(self) -> None:
        """Negative lengths other than -1 (the null sentinel) are invalid."""
        parser = CommandParser()
        parser.feed(b"*1\r\n$-2\r\n")
        with pytest.raises(ProtocolError, match="Invalid bulk string length"):
            parser.try_parse()

    def test_negative_array_count(self) -> None:
        parser = CommandParser()
        parser.feed(b"*-2\r\n")
        with pytest.raises(ProtocolError, match="Invalid array count"):
            parser.try_parse()

    def test_non_numeric_integer(self) -> None:
        parser = CommandParser()
        parser.feed(b":notanum\r\n")
        with pytest.raises(ProtocolError, match="Non-numeric integer"):
            parser.try_parse()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Binary-safety tests
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestBinarySafety:
    def test_bulk_string_with_embedded_crlf(self) -> None:
        """A bulk string whose payload contains \\r\\n must parse correctly
        because the length prefix governs how many bytes to read."""
        payload = b"line1\r\nline2"
        wire = _make_command_bytes(b"SET", b"key", payload)
        parser = CommandParser()
        parser.feed(wire)

        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "SET"
        assert cmd.args[1] == payload

    def test_bulk_string_with_null_bytes(self) -> None:
        payload = b"\x00\x01\x02\xff"
        wire = _make_command_bytes(b"SET", b"bin", payload)
        parser = CommandParser()
        parser.feed(wire)

        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.args[1] == payload

    def test_crlf_only_payload(self) -> None:
        payload = b"\r\n\r\n"
        wire = _make_command_bytes(b"SET", b"k", payload)
        parser = CommandParser()
        parser.feed(wire)

        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.args[1] == payload


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Edge cases
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


class TestEdgeCases:
    def test_empty_array(self) -> None:
        """An empty array (*0\\r\\n) is valid but yields no command."""
        parser = CommandParser()
        parser.feed(b"*0\r\n")
        # Empty arrays don't form a command; try_parse returns None.
        assert parser.try_parse() is None

    def test_empty_bulk_string_argument(self) -> None:
        """A zero-length bulk string ($0\\r\\n\\r\\n) is a valid empty arg."""
        wire = _make_command_bytes(b"SET", b"key", b"")
        parser = CommandParser()
        parser.feed(wire)

        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.args == [b"key", b""]

    def test_null_bulk_string_in_array(self) -> None:
        """A null bulk string ($-1) inside an array is valid RESP;
        but since our top-level parser expects only bytes args for
        commands, this will raise ProtocolError (non-bulk-string arg)."""
        parser = CommandParser()
        # *2\r\n$3\r\nSET\r\n$-1\r\n
        parser.feed(b"*2\r\n$3\r\nSET\r\n$-1\r\n")
        with pytest.raises(ProtocolError, match="not a bulk string"):
            parser.try_parse()

    def test_incomplete_returns_none_not_error(self) -> None:
        """Incomplete data must never raise — only return None."""
        parser = CommandParser()

        parser.feed(b"*2\r\n$3\r\nSE")
        assert parser.try_parse() is None  # no error

        parser.feed(b"T\r\n$3\r\nfoo\r\n")
        cmd = parser.try_parse()
        assert cmd is not None
        assert cmd.name == "SET"
        assert cmd.args == [b"foo"]

    def test_empty_feed(self) -> None:
        parser = CommandParser()
        parser.feed(b"")
        assert parser.try_parse() is None
