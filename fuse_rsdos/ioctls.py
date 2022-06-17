#!python3
from fcntl import ioctl
from struct import unpack

BLKGETSIZE64 = 0x80081272

def ioctl_read_uint64(fd, req):
    buf = bytearray(8)
    ioctl(fd, req, buf)
    return struct.unpack("L", buf)[0]

def ioctl_read_uint32(fd, req):
    buf = bytearray(4)
    ioctl(fd, req, buf)
    return struct.unpack("I", buf)[0]

def getsize64(fd):
    return ioctl_read_uint64(fd, BLKGETSIZE64)
