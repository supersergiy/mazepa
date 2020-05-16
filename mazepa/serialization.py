import pickle
import codecs

def serialize(obj):
    return codecs.encode(pickle.dumps(obj, protocol=4), "base64").decode()

def deserialize(s):
    return pickle.loads(codecs.decode(s.encode(), "base64"))
