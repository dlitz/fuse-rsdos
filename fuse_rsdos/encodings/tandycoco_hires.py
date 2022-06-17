#!python3
# dlitz 2022

import codecs

class Codec(codecs.Codec):

    def encode(self, input, errors='strict'):
        return codecs.charmap_encode(input, errors, encoding_table)

    def decode(self, input, errors='strict'):
        return codecs.charmap_decode(input, errors, decoding_table)

class IncrementalEncoder(codecs.IncrementalEncoder):
    def encode(self, input, final=False):
        return codecs.charmap_encode(input, self.errors, encoding_table)[0]

class IncrementalDecoder(codecs.IncrementalDecoder):
    def decode(self, input, final=False):
        return codecs.charmap_decode(input, self.errors, decoding_table)[0]

class StreamReader(Codec, codecs.StreamReader):
    pass

class StreamWriter(Codec, codecs.StreamWriter):
    pass

### encodings module API
def getregentry():
    return codecs.CodecInfo(
        name='tandycoco-hires',
        encode=Codec().encode,
        decode=Codec().decode,
        incrementalencoder=IncrementalEncoder,
        incrementaldecoder=IncrementalDecoder,
        streamreader=StreamReader,
        streamwriter=StreamWriter,
    )

### Decoding Table

def _make_decoding_table():
    result = [
    *(chr(c) for c in range(128)),
    *'ÇüéâäàåçêëèïîßÄÅóæÆôöøûùØŬÜ§£±ºƒ',        # XXX This is probably wrong
]
    return "".join(result)

decoding_table = _make_decoding_table()
encoding_table = codecs.charmap_build(decoding_table)

def codec_search_function(encoding_name):
    if encoding_name in ('tandycoco-hires', 'tandycoco_hires'):
        return getregentry()

codecs.register(codec_search_function)
