import datetime
import importlib
import json
from singledispatch import singledispatch
from utility.utilities import resolve_attr


class ObjectJSONEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError as e:
            if "not JSON serializable" not in str(e):
                raise
            return to_jsonable(obj)


@singledispatch
def to_jsonable(obj):
    d = { '__class__': obj.__class__.__qualname__,
          '__module__': obj.__module__,
        }
    d.update(obj.__dict__)
    return d


@to_jsonable.register(datetime.date)
def _(obj):
    return { 'ISO8601_date': obj.isoformat() }


@to_jsonable.register(datetime.datetime)
def _(obj):
    return { 'ISO8601_datetime': obj.isoformat() }


class ObjectJSONDecoder(json.JSONDecoder):

    def __init__(self):
        super().__init__(object_hook=ObjectJSONDecoder.from_jsonable)

    @staticmethod
    def from_jsonable(d):
        if '__class__' in d and '__module__' in d:
            return ObjectJSONDecoder._decode_class(d)
        elif 'ISO8601_date' in d:
            return ObjectJSONDecoder._decode_date(d)
        elif 'ISO8601_datetime' in d:
            return ObjectJSONDecoder._decode_datetime(d)
        return d

    @staticmethod
    def _decode_class(d):
        class_name = d.pop('__class__')
        module_name = d.pop('__module__')
        module = importlib.import_module(module_name)
        cls = resolve_attr(module, class_name)
        try:
            obj = cls(**d)
        except Exception:
            obj = cls()
            obj.__dict__.update(d)
        return obj

    @staticmethod
    def _decode_date(d):
        return datetime.datetime.strptime(d['ISO8601_date'], '%Y-%m-%d').date()

    @staticmethod
    def _decode_datetime(d):
        return datetime.datetime.strptime(d['ISO8601_date'], '%Y-%m-%dT%H:%M:%S.%f%Z')



