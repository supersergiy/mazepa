import pickle
import codecs
import zlib
import objectscriber as scriber

def serialize(obj):
    #pickled = pickle.dumps(obj, protocol=4)
    #pickled_comp = codecs.encode(zlib.compress(pickled), "base64")
    scribed = scriber.serialize(obj)
    scribed_comp = zlib.compress(scribed.encode())
    #print (f"pickled: {len(pickled)}")
    #print (f"pickled comp: {len(pickled_comp)}")
    #print (f"scribed: {len(scribed)}")
    #print (f"scribed comp: {len(scribed_comp)}")
    #import pdb; pdb.set_trace()
    return codecs.encode(scribed_comp, "base64").decode()

def deserialize(s):
    decomp = zlib.decompress(codecs.decode(s.encode(), "base64"))
    described = scriber.deserialize(decomp)
    return described
    #return data_comp.decode()
    #return pickle.loads(zlib.decompress(codecs.decode(s.encode(), "base64")))
