#!python3
# dlitz 2022

from dataclasses import dataclass
from fuse import Fuse, FuseOptParse
from pathlib import Path
from struct import pack, unpack
import errno
import fuse
import os
import sys
import threading
import warnings

from .ioctls import getsize64
from .encodings import tandycoco_hires

# Ref: http://dragon32.info/info/tandydsk.html

# 1 side
# 18 sectors per track
# 256 bytes per sector
#
# Directory is on track 17
#
# Disk is typically 35 tracks (cylinders), but we will support more.
#
# Space is allocated in 9-sector "granules".
#
# The "granule map" (allocation map) takes up Track 17, sector 2.
# The directory starts on Track 17, sector 3.
# So, the largest number of granules a filesystem can contain might be 256, but
# because of the format of the granule map, we're limited to 192.
# So the maximum raw filesystem size should be
#    192 granules * 9 sectors/granule    (user data)
#   + 18 sectors                         (directory track)
# = 1,746 sectors
# * 256 bytes/sector
# = 446,976 bytes

class Geometry:
    sectors_per_track = 18
    sectors_per_granule = 9
    bytes_per_sector = 256
    directory_track = 17
    #directory_track = 34
    directory_sectors = 9   # Sectors #3 to #11
    max_granules = 192
    directory_entry_size = 32
    sides = 1

    @property
    def max_filesystem_sectors(self):
        return self.sectors_per_granule * self.max_granules + self.sectors_per_track

    @property
    def max_filesystem_bytes(self):
        return self.max_filesystem_sectors * self.bytes_per_sector

    @property
    def max_directory_entries(self):
        return self.directory_sectors * self.bytes_per_sector // self.directory_entry_size

def ceildiv(a, b):
    return -(a // -b)

class RSDOSFSWarning(Warning):
    pass

class OddSizeWarning(RSDOSFSWarning):
    pass

class FilesystemFatalError(Exception):
    pass

class RecoverableCorruption(Exception):
    pass

class DiskFullError(Exception):
    pass

@dataclass
class DirectoryEntry:
    n:int
    filename:bytes
    extension:bytes
    filetype:int
    ascii_flag:int
    first_granule:int
    last_sector_bytes_used:int
    _reserved:bytes
    free:bool
    null:bool

    @classmethod
    def fromraw(cls, n, raw, *, geom):
        filename, extension, filetype, ascii_flag, first_granule, last_sector_bytes_used, _reserved = unpack("!8s3sBBBH16s", raw)
        null = set(raw) == { 0 }
        free = filename[0] == 0 or set(filename) == { 0xff }
        return cls(n, filename, extension, filetype, ascii_flag, first_granule, last_sector_bytes_used, _reserved, free, null)

    def pretty_filename_bytes(self):
        return self.filename.rstrip(b' ') + b'.' + self.extension.rstrip(b' ')

    def pretty_filename(self):
        return self.pretty_filename_bytes().decode('tandycoco-hires', 'surrogateescape')

@dataclass
class GranuleInfo:
    raw_value:int           # raw value in the granule map
    n:int                   # Granule number, or 0xff if the granule is free
    next_n:int              # next granule number
    sectors_used:int        # number of sectors used in this granule
    bytes_used:int          # number of bytes used in this granule
    last:bool               # True if this is the last sector number
    free:bool               # True if this granule is free
    geom:Geometry

    @classmethod
    def fromraw(cls, n, raw_value, last_sector_bytes_used, *, geom):
        if isinstance(raw_value, int):
            (b,) = bytes([raw_value])
        else:
            (b,) = bytes(raw_value)
        if 0 <= b < 0xc0:
            return cls(
                raw_value=b,
                n=n,
                next_n=b,
                sectors_used=geom.sectors_per_granule,
                bytes_used=geom.sectors_per_granule * geom.bytes_per_sector,
                last=False,
                free=False,
                geom=geom
            )
        elif 0xc0 <= b <= 0xc9:
            sectors_used = b & 0xf
            assert sectors_used <= geom.sectors_per_granule, (n, raw_value, sectors_used)
            if sectors_used == 0:
                assert last_sector_bytes_used == 0, last_sector_bytes_used
                bytes_used = 0
            else:
                bytes_used = (sectors_used - 1) * geom.bytes_per_sector + last_sector_bytes_used
            return cls(
                raw_value=b,
                n=n,
                next_n=None,
                sectors_used=sectors_used,
                bytes_used=bytes_used,
                last=True,
                free=False,
                geom=geom,
            )
        elif b == 0xff:
            return cls(
                raw_value=b,
                n=n,
                next_n=None,
                sectors_used=0,
                bytes_used=0,
                last=True,
                free=True,
                geom=geom,
            )
        else:
            raise RecoverableCorruption(f"granule #{n} corrupt: value 0x{b:02x} out of range")


# for statvfs
@dataclass
class FilesystemStats:
    total_granule_count:int
    free_granule_count:int
    used_granule_count:int
    total_direntry_count:int
    free_direntry_count:int
    used_direntry_count:int
    size_in_granules:int
    sectors_per_track:int
    sectors_per_granule:int
    bytes_per_sector:int

class RSDOSFilesystem:
    def __init__(self, io):
        self.io = io
        self.geom = Geometry()

        # lock for single-threaded operations
        self.lock = threading.Lock()

    def _getsize64(self):
        try:
            return getsize64(self.io)
        except OSError as exc:
            if exc.errno == errno.ENOTTY:
                return self._getsize64_noioctl()
            raise

    def _getsize64_noioctl(self):
        with self.lock:
            pos = self.io.tell()
            try:
                self.io.seek(0, 2)
                return self.io.tell()
            finally:
                self.io.seek(pos)

    def sector_count(self):
        n = self._getsize64()
        extraneous_bytes = -(n % -self.geom.bytes_per_sector)
        if extraneous_bytes:
            warnings.warn(f"filesystem is not an even multiple of the sector size {extraneous_bytes:d} bytes will be ignored", OddSizeWarning)
        n_sectors = n // self.geom.bytes_per_sector
        if n_sectors < (self.geom.directory_track + 1) * self.geom.sectors_per_track:
            raise FilesystemFatalError(f"filesystem is smaller than the minimum size")

        return n_sectors

    def granule_count(self):
        s = self.sector_count() - self.geom.sectors_per_track
        extraneous_sectors = -(s % -self.geom.sectors_per_granule)
        if extraneous_sectors:
            warnings.warn(f"filesystem is not an even multiple of the granule size {extraneous_sectors:d} sectors will be ignored", OddSizeWarning)
        return s // self.geom.sectors_per_granule

    def sector_range_linear(self, n, sector_count=1):
        # This uses 0-based sector numbering
        start = n * self.geom.bytes_per_sector
        end = start + self.geom.bytes_per_sector * sector_count
        return (start, end)

    def sector_range(self, cyl, secnr, sector_count=1):
        # This uses 1-based sector numbering (just like the on-disk format)
        if not 0 <= cyl:
            raise IndexError("cylinder number out of range")
        if not 1 <= secnr <= self.geom.sectors_per_track:
            raise IndexError("sector number out of range")
        if not 0 <= sector_count <= self.geom.sectors_per_track - (secnr-1):
            raise IndexError("sector count out of range")
        return self.sector_range_linear(cyl * self.geom.sectors_per_track + (secnr-1), sector_count)

    def granule_range(self, granule):
        sec_start, sec_end = self.granule_range_sectors(granule)
        start = self.sector_range_linear(sec_start)[0]
        end = self.sector_range_linear(sec_end)[0]
        return (start, end)

    def granule_range_sectors(self, granule):
        sec_start = granule * self.geom.sectors_per_granule
        sec_end = (granule + 1) * self.geom.sectors_per_granule
        if sec_start >= self.geom.directory_track * self.geom.sectors_per_track:
            # Skip over the Directory track (track 17)
            sec_start += self.geom.sectors_per_track
            sec_end += self.geom.sectors_per_track
        return (sec_start, sec_end)

    def directory_range(self):
        # Track 17, sectors 3 - 11 (9 sectors)
        return self.sector_range(self.geom.directory_track, 3, self.geom.directory_sectors)

    def directory_entry_range(self, n):
        d_start, d_end = self.sector_range(self.geom.directory_track, 3, self.geom.directory_sectors)
        dent_start = d_start + n * self.geom.directory_entry_size
        dent_end = dent_start + self.geom.directory_entry_size
        if not d_start <= dent_start < d_end or not d_start <= dent_end <= d_end:
            raise IndexError("directory entry out of range")
        return (dent_start, dent_end)

    def granule_map_range(self):
        start = self.sector_range(self.geom.directory_track, 2)[0]
        end = start + min(self.granule_count(), self.geom.max_granules)
        return (start, end)

    def pread(self, /, length, offset):
        fd = self.io.fileno()
        data = os.pread(fd, length, offset)
        assert len(data) == length, (length, offset)
        return data

    def read_granule_map(self):
        start, end = self.granule_map_range()
        return self.pread(end - start, start)

    def read_directory_entry(self, n):
        start, end = self.directory_entry_range(n)
        return self.pread(end - start, start)

    def read_granule(self, n):
        gmap_start, gmap_end = self.granule_map_range()
        start = gmap_start + n
        end = start + 1
        if not gmap_start <= start < gmap_end or not gmap_start <= end <= gmap_end:
            raise IndexError(f"granule number out of range: {n}")
        return self.pread(end - start, start)

    def count_granules(self):
        total_count = free_count = used_count = 0
        gmap = self.read_granule_map()
        for i, c in enumerate(gmap):
            gi = GranuleInfo.fromraw(i, c, self.geom.bytes_per_sector, geom=self.geom)
            if gi.free:
                free_count += 1
            else:
                used_count += 1
            total_count += 1
        return (total_count, free_count, used_count)

    def iter_directory_entries(self, *, include_free=False):
        for i in range(0, self.geom.max_directory_entries):
            raw = self.read_directory_entry(i)
            dentry = DirectoryEntry.fromraw(i, raw, geom=self.geom)
            if dentry.null:     # HACK
                continue
            if dentry.free and not include_free:
                continue
            yield dentry

    def count_directory_entries(self):
        total_count = free_count = used_count = 0
        for dentry in self.iter_directory_entries(include_free=True):
            if dentry.free:
                free_count += 1
            else:
                used_count += 1
            total_count += 1
        return (total_count, free_count, used_count)

    def iter_granules(self, first_granule, last_sector_bytes_used):
        seen = set()
        n = first_granule
        while True:
            raw_value = self.read_granule(n)
            gi = GranuleInfo.fromraw(n, raw_value, last_sector_bytes_used, geom=self.geom)
            yield gi
            seen.add(n)
            if gi.last:
                break
            n = gi.next_n
            if n in seen:
                raise RecoverableCorruption("loop in granule map")

    def iter_dentry_granules(self, dentry):
        return self.iter_granules(dentry.first_granule, dentry.last_sector_bytes_used)

    def compute_file_offset(self, dentry, offset):
        granule_offset, x = divmod(offset, self.geom.bytes_per_sector * self.geom.sectors_per_granule)
        sector_offset, byte_offset = divmod(x, self.geom.bytes_per_sector)
        return (granule_offset, sector_offset, byte_offset)

    def iter_file_contents(self, dentry, length=None, offset=0):
        g0, b0 = divmod(offset, self.geom.bytes_per_sector * self.geom.sectors_per_granule)
        bytes_remaining = length
        for gi in self.iter_dentry_granules(dentry):
            assert not gi.free
            if gi.n < g0:
                continue
            sec_start, sec_end = self.granule_range_sectors(gi.n)
            start, end = self.sector_range_linear(sec_start, gi.sectors_used)
            data = self.pread(end - start, start)
            data = data[:gi.bytes_used]
            if gi.n == g0:
                data = data[b0:]
            if bytes_remaining is not None and len(data) > bytes_remaining:
                data = data[:bytes_remaining]
            yield data
            bytes_remaining -= len(data)

    def pread_file(self, dentry, length, offset):
        result = []
        for chunk in self.iter_file_contents(dentry, length=length, offset=offset):
            result.append(chunk)
        return b''.join(result)

    def directory_lookup(self, name):
        for dentry in self.iter_directory_entries():
            if dentry.pretty_filename() == name:
                return dentry
        return None

    def get_file_sizes(self, dentry):
        logical_size = 0
        allocated_granules = 0
        for gi in self.iter_dentry_granules(dentry):
            logical_size += gi.bytes_used
            allocated_granules += 1
        allocated_sectors = allocated_granules * self.geom.sectors_per_granule
        return logical_size, self.geom.bytes_per_sector, allocated_sectors

    def get_sector_size(self):
        return self.geom.bytes_per_sector

    def get_directory_size(self):
        return self.geom.directory_sectors * self.geom.bytes_per_sector

    def get_fs_stats(self):
        g_total, g_free, g_used = self.count_granules()
        d_total, d_free, d_used = self.count_directory_entries()
        return FilesystemStats(
            total_granule_count=g_total,
            free_granule_count=g_free,
            used_granule_count=g_used,
            total_direntry_count=d_total,
            free_direntry_count=d_free,
            used_direntry_count=d_used,
            size_in_granules=ceildiv(self.sector_count(), sectors_per_granule),
            sectors_per_track=self.geom.sectors_per_track,
            sectors_per_granule=self.geom.sectors_per_granule,
            bytes_per_sector=self.geom.bytes_per_sector,
        )
