# (C) Copyright sciota GmbH 2024.
# Released under the MIT license.

import time
import minipb
import json
import struct


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

def _get_metric_value(metric, datatype=None):
    if 'datatype' in metric:
        datatype = metric['datatype']
    
    if datatype is None:
        print("Datatype not provided.\nValue could not be returned.")
        return None
    
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
    elif datatype == DataType.Int8Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes), 'b'), received_bytes)
    elif datatype == DataType.Int16Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 2, 'h'), received_bytes)
    elif datatype == DataType.Int32Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 4, 'i'), received_bytes)
    elif datatype == DataType.Int64Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 8, 'q'), received_bytes)
    elif datatype == DataType.UInt8Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes), 'B'), received_bytes)
    elif datatype == DataType.UInt16Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 2, 'H'), received_bytes)
    elif datatype == DataType.UInt32Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 4, 'I'), received_bytes)
    elif datatype == DataType.UInt64Array:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 8, 'Q'), received_bytes)
    elif datatype == DataType.FloatArray:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 4, 'f'), received_bytes)
    elif datatype == DataType.DoubleArray:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 8, 'd'), received_bytes)
    elif datatype == DataType.BooleanArray:
        received_bytes = metric['bytes_value']
        # unpack the 4-byte integer representing the number of boolean values
        boolean_count, = struct.unpack("<I", received_bytes[:4])
        # unpack the rest of the bytes into a list of boolean values
        bits = ''.join(f'{byte:08b}' for byte in received_bytes[4:])[:boolean_count]
        return [bool(int(bit)) for bit in bits]
    elif datatype == DataType.StringArray:
        received_bytes = metric['bytes_value']
        return received_bytes.decode('utf-8').split('\0')[:-1]
    elif datatype == DataType.DateTimeArray:
        received_bytes = metric['bytes_value']
        return struct.unpack('<{}{}'.format(len(received_bytes) // 8, 'q'), received_bytes)
    else:
        print(f"Unsupported datatype: {datatype}\nValue not returned.")
        return None
    
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
        # TODO: elif datatype == DataType.Template:
        elif datatype == DataType.Int8Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'b'), *value)
        elif datatype == DataType.Int16Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'h'), *value)
        elif datatype == DataType.Int32Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'i'), *value)
        elif datatype == DataType.Int64Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'q'), *value)
        elif datatype == DataType.UInt8Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'B'), *value)
        elif datatype == DataType.UInt16Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'H'), *value)
        elif datatype == DataType.UInt32Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'I'), *value)
        elif datatype == DataType.UInt64Array:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'Q'), *value)
        elif datatype == DataType.FloatArray:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'f'), *value)
        elif datatype == DataType.DoubleArray:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'd'), *value)
        elif datatype == DataType.BooleanArray:
            # pack the number of boolean values into a 4-byte integer
            boolean_count_bytes = struct.pack("<I", len(value))
            # pack the boolean values into bytes
            binary_string = ''.join(str(int(b)) for b in value)
            binary_string_padded = ''.join((binary_string, '0' * (8 * ((len(binary_string) + 7) // 8) - len(binary_string))))
            boolean_values_bytes = bytes(int(binary_string_padded[i:i+8], 2) for i in range(0, len(binary_string_padded), 8))
            metric['bytes_value'] = boolean_count_bytes + boolean_values_bytes
        elif datatype == DataType.StringArray:
            metric['bytes_value'] = '\0'.join(value).encode('utf-8') + '\0'
        elif datatype == DataType.DateTimeArray:
            metric['bytes_value'] = struct.pack('<{}{}'.format(len(value), 'q'), *value)
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