import io
from typing import (
    BinaryIO,
)


class BufferReader(io.RawIOBase, BinaryIO):
    """
    A seekable binary stream over a buffer, similar to :class:`io.BytesIO`,
    with the following differences:

    - it uses the given buffer directly instead of copying it first
    - it accepts a mutable as well as an immutable buffer
    - it is read-only

    ``BinaryIO`` is a base class solely to satisfy consumers annotated with
    ``IO``. Typeshed grafts it onto the concrete stream classes it declares,
    for that same reason, but not onto :class:`io.RawIOBase`.

    >>> buffer = bytearray(b'0123456789')
    >>> reader = BufferReader(buffer)
    >>> reader.readable(), reader.seekable(), reader.writable()
    (True, True, False)

    >>> reader.read(4), reader.tell()
    (b'0123', 4)

    >>> reader.read(), reader.read()
    (b'456789', b'')

    >>> reader.seek(2), reader.read(3)
    (2, b'234')

    >>> reader.seek(-3, io.SEEK_END), reader.read()
    (7, b'789')

    >>> reader.seek(-2, io.SEEK_CUR), reader.read()
    (8, b'89')

    The buffer is not copied, so writing to it affects what is read next.

    >>> _ = reader.seek(0)
    >>> buffer[0:1] = b'x'
    >>> reader.read(2)
    b'x1'

    An immutable buffer can be read just the same.

    >>> BufferReader(b'0123456789').read()
    b'0123456789'

    """

    def __init__(self, buffer: bytes | bytearray):
        super().__init__()
        # We could use buffer directly and read() could return slices of the
        # buffer, but slices on bytes and bytearray objects involve a copy.
        # Avoiding that copy could be crucial when a consumer reads a large
        # range in one call.
        self._view = memoryview(buffer)
        self._offset = 0

    def readable(self) -> bool:
        return True

    def seekable(self) -> bool:
        return True

    def tell(self) -> int:
        return self._offset

    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_SET:
            position = offset
        elif whence == io.SEEK_CUR:
            position = self._offset + offset
        elif whence == io.SEEK_END:
            position = len(self._view) + offset
        else:
            raise ValueError('Invalid whence', whence)
        assert 0 <= position <= len(self._view), position
        self._offset = position
        return position

    def readinto(self, buffer) -> int:
        # Assigning to a slice of a `bytearray` copies the right-hand side into
        # a `bytearray` first, unless it already is one. Assigning to a slice of
        # a view of the destination doesn't, whatever the destination is.
        rhs_view = memoryview(buffer)
        size = min(len(rhs_view), len(self._view) - self._offset)
        lhs_view = self._view[self._offset:self._offset + size]
        rhs_view[:size] = lhs_view
        self._offset += size
        return size
