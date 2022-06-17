#!/usr/bin/env python3
# dlitz 2022

import fuse
import os
import sys

from .fusefs import RSDOSFuseFilesystem

def main():
    server = RSDOSFuseFilesystem(prog=os.path.basename(sys.argv[0]))
    server.parse()
    server.main()

if __name__ == '__main__':
    main()
