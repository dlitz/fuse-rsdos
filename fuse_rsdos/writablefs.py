# dlitz 2022
# WIP XXX

from .rsdosfs import RSDOSFilesystem, DirectoryEntry, GranuleInfo
from .rsdosfs import *

class WritableRSDOSFilesystem(RSDOSFilesystem):
    def _find_free_direntry(self):
        assert self.lock.locked()
        for direntry in self.iter_directory_entries(include_free=True):
            if not direntry.free:
                continue
            return direntry
        else:
            raise DiskFullError

    def _find_free_granules(self, count):
        assert self.lock.locked()
        result = []
        gmap = self.read_granule_map()
        for n, c in enumerate(gmap):
            gi = GranuleInfo.fromraw(n, c, bytes_per_sector)
            if gi.free:
                result.append(n)
                if len(result) == count:
                    return result
        else:
            raise DiskFullError

    def write_new_file(self, filename, extension, filetype, ascii_flag, content):
        content = bytes(content)
        length = len(content)
        with self.lock:
            self._write_new_file(filename, extension, filetype, ascii_flag, content)

    def _write_new_file(self, filename, extension, filetype, ascii_flag, content):
        assert self.lock.locked()
        granules_needed = ceildiv(length, bytes_per_sector * sectors_per_granule)
        granules = self._find_free_granules(granules_needed)
        direntry = self._find_free_direntry()
        direntry.clear()
        direntry.filename = filename
        direntry.extension = extension
        direntry.filetype = filetype
        direntry.ascii_flag = ascii_flag
        direntry.last_sector_bytes_used = -(len(content) % -self.geom.bytes_per_sector)
        if direntry.last_sector_bytes_used == 0:
            direntry.last_sector_bytes_used = self.geom.bytes_per_sector

        sector_count = ceildiv(len(content), self.geom.bytes_per_sector)
        granule_count = ceildiv(sector_count, self.geom.sectors_per_granule)
        last_granule_sector_count = -(sector_count % -self.geom.sectors_per_granule)
        if last_granule_sector_count == 0:
            last_granule_sector_count = self.geom.sectors_per_granule

        granule_values = {}
        for i, n in reversed(enumerate(granules)):
            if i == len(granules)-1:
                gval = 0xc0 | last_granule_sector_count
            else:
                gval = granules[i+1]
            granule_values[n] = gval
        direntry.first_granule = granules[0]

        gm_start, gm_end = self.granule_map_range()
        for n in granules:
            self.pwrite(bytes([granule_values[n]]), gm_start + n)

        for i, g in enumerate(granules):
            g_start, g_end = self.granule_range(g)
            size = g_end - g_start
            data = content[self.geom.bytes_per_granule*i:self.geom.bytes_per_granule*(i+1)]
            if i+1 == len(granules):
                pad_needed = -(len(data) % -self.geom.bytes_per_granule)
                data += '\xff' * pad_needed
            self.pwrite(data, g_start)

        d_start, d_end = self.directory_entry_range(direntry.n)
        self.pwrite(direntry.toraw(), d_start)

    def pwrite(self, /, buf, offset):
        fd = self.io.fileno()
        return os.pwrite(fd, buf, offset)
