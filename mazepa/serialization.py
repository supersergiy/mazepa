import pickle
import codecs
import zlib

def serialize(obj):
    return codecs.encode(zlib.compress(pickle.dumps(obj, protocol=4)), "base64").decode()

def deserialize(s):
    return pickle.loads(zlib.decompress(codecs.decode(s.encode(), "base64")))
