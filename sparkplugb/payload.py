# (C) Copyright sciota GmbH 2024.
# Released under the MIT license.

import time
import minipb
import json


_datasetvalue_schema = (
    ('int_value', 'T'),
    ('long_value', 'T'),
    ('float_value', 'f'),
    ('double_value', 'd'),
    ('boolean_value', 'b'),
    ('string_value', 'U'),
)

_row_schema = (
    ('elements', '+[', _datasetvalue_schema), # extra comma is needed so that this is a tuple
)

_dataset_schema = (
    ('num_of_columns', 'T'),
    ('columns', '+U'),
    ('types', '+T'),
    ('rows', '+[', _row_schema),
)

_metric_schema = (
    ('name', 'U'),
    ('alias', 'T'),
    ('timestamp', 'T'),
    ('datatype', 'T'),
    ('is_historical', 'b'),
    ('is_transient', 'b'),
    ('is_null', 'b'),
    ('metadata', 'x'),
    # TODO: ('metadata', metadata_schema),
    ('properties', 'x'),
    # TODO: ('properties', property_set_schema),
    ('int_value', 'T'),
    ('long_value', 'T'),
    ('float_value', 'f'),
    ('double_value', 'd'),
    ('boolean_value', 'b'),
    ('string_value', 'U'),
    ('bytes_value', 'a'),
    ('dataset_value', _dataset_schema),
    # TODO: ('template_value', template_schema),
)

_payload_schema = (
    ('timestamp', 'T'),
    ('metrics', '+[', _metric_schema),
    ('seq', 'T'),
    ('uuid', 'U'),
    ('body', 'a')
)

class DataType:
    # Indexes of Data Types
    Unknown         = 0

    # Basic Types
    Int8            = 1
    Int16           = 2
    Int32           = 3
    Int64           = 4
    UInt8           = 5
    UInt16          = 6
    UInt32          = 7
    UInt64          = 8
    Float           = 9
    Double          = 10
    Boolean         = 11
    String          = 12
    DateTime        = 13
    Text            = 14

    # Additional Metric Types
    UUID            = 15
    DataSet         = 16
    Bytes           = 17
    File            = 18
    Template        = 19

    # Additional PropertyValue Types
    PropertySet     = 20
    PropertySetList = 21

    # Array Types
    Int8Array = 22
    Int16Array = 23
    Int32Array = 24
    Int64Array = 25
    UInt8Array = 26
    UInt16Array = 27
    UInt32Array = 28
    UInt64Array = 29
    FloatArray = 30
    DoubleArray = 31
    BooleanArray = 32
    StringArray = 33
    DateTimeArray = 34

def _get_metric_value(metric):
    datatype = metric['datatype']
    
    if datatype == DataType.Int8:
        return metric['int_value']
    elif datatype == DataType.Int16:
        return metric['int_value']
    elif datatype == DataType.Int32:
        return metric['int_value']
    elif datatype == DataType.Int64:
        return metric['long_value']
    elif datatype == DataType.UInt8:
        return metric['int_value']
    elif datatype == DataType.UInt16:
        return metric['int_value']
    elif datatype == DataType.UInt32:
        return metric['int_value']
    elif datatype == DataType.UInt64:
        return metric['long_value']
    elif datatype == DataType.Float:
        return metric['float_value']
    elif datatype == DataType.Double:
        return metric['double_value']
    elif datatype == DataType.Boolean:
        return metric['boolean_value']
    elif datatype == DataType.String:
        return metric['string_value']
    elif datatype == DataType.DateTime:
        return metric['long_value']
    elif datatype == DataType.Text:
        return metric['string_value']
    elif datatype == DataType.UUID:
        return metric['string_value']
    elif datatype == DataType.DataSet:
        return metric['dataset_value']
    elif datatype == DataType.Bytes:
        return metric['bytes_value']
    elif datatype == DataType.File:
        return metric['bytes_value']
    # TODO: elif datatype == DataType.Template:

def _fill_empty_fields_with_none(data, schema):
    for (key, *_) in schema:
        if key not in data:
            data[key] = None
    return data

class DataSet:
    def __init__(self, columns: list[str], types: list):
        self.num_of_columns = len(columns)
        self.columns = columns
        self.types = types
        self.rows = []
        
    def add_row(self, elements: list):
        if len(elements) != self.num_of_columns:
            print(f"Number of elements in the row does not match the number of columns.\nRow not added.")
            return
        elements = [self._get_element(value, datatype) for value, datatype in zip(elements, self.types)]
        row = {
            'elements': elements
        }
        self.rows.append(row)
        
    def _get_element(self, value, datatype):
        element = {}
        
        if datatype == DataType.Int8:
            element['int_value'] = value
        elif datatype == DataType.Int16:
            element['int_value'] = value
        elif datatype == DataType.Int32:
            element['int_value'] = value
        elif datatype == DataType.Int64:
            element['long_value'] = value
        elif datatype == DataType.UInt8:
            element['int_value'] = value
        elif datatype == DataType.UInt16:
            element['int_value'] = value
        elif datatype == DataType.UInt32:
            element['int_value'] = value
        elif datatype == DataType.UInt64:
            element['long_value'] = value
        elif datatype == DataType.Float:
            element['float_value'] = value
        elif datatype == DataType.Double:
            element['double_value'] = value
        elif datatype == DataType.Boolean:
            element['boolean_value'] = value
        elif datatype == DataType.String:
            element['string_value'] = value
        elif datatype == DataType.DateTime:
            element['long_value'] = value
        elif datatype == DataType.Text:
            element['string_value'] = value
        elif datatype == DataType.UUID:
            element['string_value'] = value
        else:
            print(f"Unsupported DataSetValue datatype: {datatype}\nElement not added.")
            return None
        
        element = _fill_empty_fields_with_none(element, _datasetvalue_schema)
        return element
        
    def _get_dict(self):
        return _fill_empty_fields_with_none({
            'num_of_columns': self.num_of_columns,
            'columns': self.columns,
            'types': self.types,
            'rows': self.rows
        }, _dataset_schema)

# The ESP32 (and probably some other platforms) use a different epoch year (2000)
_epoch_year = time.gmtime(0)[0]
_millis_between_1970_and_2000 = 946684800000
_timestamp_correction = _millis_between_1970_and_2000 if _epoch_year == 2000 else 0

class _Payload:
    def __init__(self, use_timestamp: bool = True, seq: int = None):
        self.metrics = []
        self.timestamp = None
        if use_timestamp:
            self.timestamp = time.time_ns() // 1000000 + _timestamp_correction
            
        self.seq = seq
        
    def add_metric(self, name: str, datatype: int, value):
        metric = {
            'name': name,
            'timestamp': time.time_ns() // 1000000 + _timestamp_correction,
            'datatype': datatype,
        }
        
        if datatype == DataType.Int8:
            metric['int_value'] = value
        elif datatype == DataType.Int16:
            metric['int_value'] = value
        elif datatype == DataType.Int32:
            metric['int_value'] = value
        elif datatype == DataType.Int64:
            metric['long_value'] = value
        elif datatype == DataType.UInt8:
            metric['int_value'] = value
        elif datatype == DataType.UInt16:
            metric['int_value'] = value
        elif datatype == DataType.UInt32:
            metric['int_value'] = value
        elif datatype == DataType.UInt64:
            metric['long_value'] = value
        elif datatype == DataType.Float:
            metric['float_value'] = value
        elif datatype == DataType.Double:
            metric['double_value'] = value
        elif datatype == DataType.Boolean:
            metric['boolean_value'] = value
        elif datatype == DataType.String:
            metric['string_value'] = value
        elif datatype == DataType.DateTime:
            metric['long_value'] = value
        elif datatype == DataType.Text:
            metric['string_value'] = value
        elif datatype == DataType.UUID:
            metric['string_value'] = value
        elif datatype == DataType.DataSet:
            if not isinstance(value, DataSet):
                print("Value is not an instance of DataSet.\nMetric not added.")
                return
            metric['dataset_value'] = value._get_dict()
        elif datatype == DataType.Bytes:
            metric['bytes_value'] = value
        elif datatype == DataType.File:
            metric['bytes_value'] = value
        # elif datatype == DataType.Template:
            # TODO: Implement
            # pass
        # TODO Array types
        else:
            print(f"Unsupported datatype: {datatype}\nMetric not added.")
            return
        
        metric = _fill_empty_fields_with_none(metric, _metric_schema)
        self.metrics.append(metric)
    
    def get_dict(self):
        payload_dict = {
            'metrics': self.metrics
        }
        if self.timestamp:
            payload_dict['timestamp'] = self.timestamp
        if self.seq is not None:
            payload_dict['seq'] = self.seq
            
        return _fill_empty_fields_with_none(payload_dict, _payload_schema)

_w = minipb.Wire(_payload_schema)

def _encode_payload(payload: _Payload):
    return _w.encode(payload.get_dict())

def _decode_payload(payload: bytes):
    return _w.decode(payload)

def _decode_host_application_state_payload(payload: bytes):
    return json.loads(payload)