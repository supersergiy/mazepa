from objectscriber import Scriber

mazepa_scriber = Scriber()

def serializable(cls):
    return mazepa_scriber.register_class(cls)

def serialize(obj):
    return mazepa_scriber.serialize(obj)

def deserialize(s):
    return mazepa_scriber.deserialize(s)
