#!python3
# dlitz 2022

from fuse import Fuse, FuseOptParse
from pathlib import PurePath
from functools import wraps
import errno
import fuse
import os
import stat
import sys
import re

from .rsdosfs import RSDOSFilesystem

fuse.fuse_python_api = (0, 2)
fuse.feature_assert('stateful_files', 'has_init')       # TODO - what do I want here

def _make_fields_sequence(func, size, prefix):
    sr = func(range(size))
    dd = {name: getattr(sr, name) for name in dir(sr) if name.startswith(prefix)}
    items = sorted(dd.items(), key=lambda kv: kv[1])
    return [k for k, v in items]
_stat_result_fields_sequence = _make_fields_sequence(os.stat_result, 19, 'st_')
_statvfs_result_fields_sequence = _make_fields_sequence(os.statvfs_result, 11, 'f_')

def make_stat_result(**kw):
    field_values = []
    for name in _stat_result_fields_sequence:
        field_values.append(kw.pop(name, None))
    assert not kw, kw
    return os.stat_result(field_values)

def make_statvfs_result(**kw):
    field_values = []
    for name in _statvfs_result_fields_sequence:
        field_values.append(kw.pop(name, None))
    assert not kw, kw
    return os.statvfs_result(field_values)

def ceildiv(a, b):
    return -(a // -b)

def traceback_wrap(func):
    @wraps(func)
    def wrapper(*args, **kw):
        try:
            return func(*args, **kw)
        except BaseException:
            import traceback
            traceback.print_exc()
            raise
    return wrapper

class RSDOSOptParse(FuseOptParse):
    def __init__(self, *args, **kw):
        if 'fetch_dev' in kw:
            self.fetch_dev = bool(kw.pop('fetch_dev'))
        else:
            self.fetch_dev = True
        super().__init__(*args, **kw)

        if self.fetch_dev:
            self.add_option(mountopt='device', metavar='DEVICE',
                help='file or block device to mount')

    def parse_args(self, args=None, values=None):
        o, a = super().parse_args(args, values)
        if a and self.fetch_dev:
            self.fuse_args.device = os.path.realpath(a.pop())
        return o,a

    def assemble(self):
        assert 0, "ASSEMBLE!"
        args = super().assemble()
        if self.fetch_dev:
            args.insert(0, self.fuse_args.device)
        return args


class RSDOSFuseFile:
    fusefs = None

    @property
    def fs(self):
        return self.fusefs.fs

    def __init__(self, path, flags, *mode):
        self.dentry = self.fusefs._lookup(path)
        self.path = PurePath(path)
        self.flags = flags
        self.mode = mode
        print(f"RSDOSFuseFile(fusefs={self.fusefs!r}, {path=!r}, {flags=!r}, *{mode=!r})")

    def read(self, length, offset):
        return self.fs.pread_file(self.dentry, length, offset)

    def release(self, flags):
        pass


class RSDOSFuseFilesystem(Fuse):
    usage = """
%prog device mountpoint [options]

Userspace RS-DOS (Disk Extended Color BASIC) filesystem.
"""

    INODE_ROOT = 1
    INODE_USER_BASE = 2

    def __init__(self, *args, **kw):
        self._set_defaults(kw)
        super().__init__(*args, **kw)

    def main(self, *a, **kw):
        self.file_class = type('RSDOSFuseFile-Class', (RSDOSFuseFile,), dict(fusefs=self))
        return super().main(*a, **kw)

    @traceback_wrap
    def fsinit(self):
        self._open_device()

    def _set_defaults(self, kw):
        kw.setdefault('usage', self.usage)
        kw.setdefault('version', f'%prog {fuse.__version__}')
        kw.setdefault('dash_s_do', 'setsingle')
        kw.setdefault('fetch_mp', True)
        kw.setdefault('fetch_dev', True)
        kw.setdefault('parser_class', RSDOSOptParse)

    def parse(self, *args, **kw):
        kw.setdefault('values', self)
        kw.setdefault('errex', 1)
        return super().parse(*args, **kw)

    def _open_device(self):
        self.device = open(self.fuse_args.device, 'rb')
        self.fs = RSDOSFilesystem(self.device)

    def _lookup(self, path):
        p = PurePath(path).relative_to('/')
        if '/' in str(p):       # we don't support subdirectories right now
            dentry = None
        else:
            dentry = self.fs.directory_lookup(str(p))
        if dentry is None:
            raise OSError(errno.ENOENT, os.strerror(errno.ENOENT) + ": " + path)
        return dentry

    @traceback_wrap
    def getattr(self, path):
        if path == '/':
            return make_stat_result(
                st_mode=stat.S_IFDIR | 0o555,
                st_ino=self.INODE_ROOT,
                st_dev=0,
                st_nlink=3,
                st_uid=0,
                st_gid=0,
                st_size=self.fs.get_directory_size(),
                st_atime=0,
                st_mtime=0,
                st_ctime=0,
                st_atime_ns=0,
                st_mtime_ns=0,
                st_ctime_ns=0,
                st_blksize=self.fs.get_sector_size(),
                st_blocks=ceildiv(self.fs.get_directory_size(), 512),
                st_rdev=0,
            )

        dentry = self._lookup(path)
        logical_size, block_size, allocated_blocks = self.fs.get_file_sizes(dentry)
        return make_stat_result(
            st_mode=stat.S_IFREG | 0o444,
            st_ino=self.INODE_USER_BASE + dentry.n,
            st_dev=0,
            st_nlink=3,
            st_uid=0,
            st_gid=0,
            st_size=logical_size,
            st_atime=0,
            st_mtime=0,
            st_ctime=0,
            st_atime_ns=0,
            st_mtime_ns=0,
            st_ctime_ns=0,
            st_blksize=block_size,
            st_blocks=ceildiv(block_size * allocated_blocks, 512),
            st_rdev=0,
        )

    @traceback_wrap
    def statfs(self):
        fst = self.fs.get_fs_stats()
        result = make_statvfs_result(
            f_bsize=fst.bytes_per_sector,
            f_frsize=fst.bytes_per_sector,
            f_blocks=self.fs.sector_count(),
            f_bfree=fst.free_granule_count * fst.sectors_per_granule,
            f_bavail=fst.free_granule_count * fst.sectors_per_granule,
            f_files=fst.total_direntry_count + self.INODE_USER_BASE - 1,
            f_ffree=fst.free_direntry_count,
            f_favail=fst.free_direntry_count,
            f_flag=0,
            #f_namemax=12,
            f_namemax=255,
        )
        return result

    @traceback_wrap
    def readdir(self, path, offset):
        print(f"readdir({path=!r}, {offset=!r})")
        for dentry in self.fs.iter_directory_entries():
            fuse_dentry = fuse.Direntry(
                dentry.pretty_filename(),
                type=stat.S_IFREG,
                ino=self.INODE_USER_BASE + dentry.n)
            print(">>>", fuse_dentry, repr(dentry.pretty_filename()), self.INODE_USER_BASE + dentry.n)
            yield fuse_dentry

    def _access_modestr(self, mode):
        modestr = []
        if mode & os.R_OK:
            modestr.append('R_OK')
        if mode & os.W_OK:
            modestr.append('W_OK')
        if mode & os.X_OK:
            modestr.append('X_OK')
        if not modestr:
            assert mode == os.F_OK
            modestr.append('F_OK')
        modestr = '|'.join(modestr)
        return modestr

    @traceback_wrap
    def access(self, path, mode):
        print(f"access({path!r}, {self._access_modestr(mode)})")
        if path != '/':
            self._lookup(path)
        if mode & os.W_OK:
            # Function not implemented
            raise OSError(errno.ENOSYS, os.strerror(errno.ENOSYS))
        if mode & os.X_OK and path != '/':
            raise OSError(errno.EPERM, os.strerror(errno.EPERM))
        return 0
