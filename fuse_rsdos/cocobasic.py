#!python3
# dlitz 2022
#
# Ref: http://dragon32.info/info/basicfmt.html
# Ref: http://dragon31.info/info/cocotokn.html

from struct import unpack
from .cocobasic_tokens import op_tokens, func_tokens
import warnings

class ExtraJunkAtEOFWarning(Warning):
    pass

class CoCo3BasicReader:

    def __init__(self, infile, *, base_address=None):
        if base_address is None:
            base_address = 0x1e01
        self.infile = infile
        self.bytes_read = 0
        self.header = None
        self.base_address = base_address
        self.ptr_next_line = base_address
        self.line_num = None

    def _ensure_read_header(self):
        if self.header:
            return
        flag = self.infile.read(1)
        assert flag == b'\xff'
        length = unpack("!H", self.infile.read(2))[0]
        self.header = (flag, length)
        #print("_ensure_read_header", flag, length)

    def read(self, count):
        data = self.infile.read(count)
        self.bytes_read += len(data)
        return data

    def iter_lines(self):
        line = self.next_line()
        while line is not None:
            yield line
            line = self.next_line()

    def iter_tokens(self):
        token = self.next_token()
        while token is not None:
            #print(">>", token)
            yield token
            token = self.next_token()

    def next_line(self):
        self._ensure_read_header()
        pos = self.ptr_next_line - self.base_address
        #assert pos == self.bytes_read, (pos, self.bytes_read)
        #print(">>", pos, self.bytes_read)
        h = self.read(2)
        if not h or len(h) < 2:
            raise EOFError
        (self.ptr_next_line,) = unpack('>H', h)
        if self.ptr_next_line == 0:  # end of program
            #if self.read(1) != b'':
            #    self.bytes_read -= 1
            #    self.infile.seek(-1, 1)
            #    warnings.warn("Extra junk at EOF", ExtraJunkAtEOFWarning)
            assert self.read(1) == b''
            assert self.bytes_read == self.header[1], (self.bytes_read, self.header)
            return None
        h = self.read(2)
        if not h or len(h) < 2:
            raise EOFError
        (self.line_num,) = unpack(">H", h)
        #print(hex(self.ptr_next_line), hex(line_num))
        line_tokens = [f'{self.line_num} ']
        for token in self.iter_tokens():
            line_tokens.append(token)
        return "".join(line_tokens)

    def next_token(self):
        assert self.infile.tell() == self.bytes_read + 3
        data = self.read(1)
        if not data:
            raise EOFError
        (b,) = data
        if b == 0:
            return None
        elif 0 < b <= 0x7f:
            return chr(b)
        elif b == 0xff:
            data2 = self.read(1)
            if not data2:
                raise EOFError
            (b2,) = data2
            assert b2 in func_tokens, (hex(b2), self.infile.tell(), self.line_num)
            return func_tokens[b2]
        else:
            assert b in op_tokens, (hex(b), self.infile.tell(), self.line_num)
            return op_tokens[b]

if __name__ == '__main__':
    from pathlib import Path
    import argparse
    import pprint
    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument('infile', type=Path)
        args = parser.parse_args()
        return args, parser
    def main():
        args, parser = parse_args()
        with open(args.infile, 'rb') as infile:
            p = CoCo3BasicReader(infile)
            for line in p.iter_lines():
                print(line)
    main()
