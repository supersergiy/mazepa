import pickle
import codecs
import zlib
import objectscriber as scriber

def serialize(obj):
    '''data = pickle.dumps(obj, protocol=4)
    data_comp = codecs.encode(zlib.compress(data), "base64")
    import pdb; pdb.set_trace()
    scirbed = scriber.serialize(obj)'''
    scribed = scriber.serialize(obj)
    scribed_comp = zlib.compress(scribed.encode())
    return codecs.encode(scribed_comp, "base64").decode()

def deserialize(s):
    decomp = zlib.decompress(codecs.decode(s.encode(), "base64"))
    described = scriber.deserialize(decomp)
    return described
    #return data_comp.decode()
    #return pickle.loads(zlib.decompress(codecs.decode(s.encode(), "base64")))
