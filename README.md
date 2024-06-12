# micropython-sparkplugb

A MicroPython compatible implementation of the Eclipse Sparkplug B Specification. Supports the usage as a Sparkplug B Edge Node without devices.

## Features

- Sparkplug B Edge Node implementation
- Management of Sparkplug B session state, incl. keeping track of bdSeq, seq, and timestamp values needed for Sparkplug messages
- Sparkplug Specification compliant sending of NBIRTH, NDEATH, and NDATA messages
- Checking for and receiving MQTT messages concurrently with custom application code using `asyncio`
- Execution of user defined callback functions on receipt of NCMD messages
- Report-By-Exception (RBE) support for metric value changes
- Waiting for a user specified primary host application to be online before sending Sparkplug messages

## Setup

### Installation

#### Using the MicroPython REPL

```python
import mip
mip.install('github:sciotaio/micropython-sparkplugb')
```

#### Using `mip` on the Unix port

```bash
./micropython -m mip install github:sciotaio/micropython-sparkplugb
```

#### Using `mpremote`

```bash
mpremote mip install github:sciotaio/micropython-sparkplugb
```

#### Manual Installation

You can install this library manually by copying the folder `sparkplugb` and its two files, `edge_node.py` and `payload.py`, to your MicroPython device.

When installing manually, you also need to install the dependencies:
- `minipb`, by running `mip.install("github:dogtopus/minipb/minipb.py")` or by copying the file `minipb.py` from [minipb](https://github.com/dogtopus/minipb) to your device.
- `logging`, which is a dependency of `minipb`, by running `mip.install("logging")` or by copying the file `python-stdlib/logging/logging.py` from [micropython-lib](https://github.com/micropython/micropython-lib) to your device.
- `bisect`, which is a dependency of `minipb`, by running `mip.install("bisect")` or by copying the file `python-stdlib/bisect/bisect.py` from [micropython-lib](https://github.com/micropython/micropython-lib) to your device.

### Choosing an MQTT Client

This library does not include an MQTT client. You will need to install one separately or check whether your MicroPython port has one built-in. We recommend using `umqtt.robust` or `umqtt.simple` from the [micropython-lib](https://github.com/micropython/micropython-lib) repository as this library has been built with these clients in mind. You can install them using `mip`:

```python
import mip
mip.install('umqtt.simple')
mip.install('umqtt.robust')
```

If you want to use a different MQTT client, you will need to either:
- modify the `edge_node.py` file to use the client of your choice, or
- create a wrapper class for your MQTT client that implements the same methods as `umqtt.simple` or `umqtt.robust`.

## Usage Example

The following is an example of how to use this library to create a Sparkplug B Edge Node with a float metric and a DataSet metric. For documentation on the API, see the [API section](#api).

First, import the library:
    
```python
from sparkplugb.edge_node import SparkplugBEdgeNode
from sparkplugb.payload import DataType, DataSet
```

Next, create an instance of your MQTT client and configure it as you like. Here is an example using `umqtt.simple` with TLS:

```python
from umqtt.simple import MQTTClient
import ssl

server = "your.broker.com"
client_id = "your_client_id"

ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)

keyfile = './certs/mqtt-client-key.der'
certfile = './certs/mqtt-client-cert.der'
cafile='./certs/broker-cert.der'

ssl_ctx.load_cert_chain(certfile, keyfile)
ssl_ctx.load_verify_locations(cafile)
ssl_ctx.verify_mode = ssl.CERT_REQUIRED

client = MQTTClient(client_id, server, 8883, ssl=ssl_ctx, keepalive=60)
```	

Then, create an instance of the Sparkplug B Edge Node and add metrics to it. Here is an example with a float metric, a DataSet metric and a command metric:

```python
node = SparkplugBEdgeNode(client, "your_group_id", "your_edge_node_id", "your_primary_host_application_id")

node.add_data_metric("Properties/Temperature", DataType.Float, 12.5)

dataset = DataSet(["Name", "Value"], [DataType.String, DataType.Float])
dataset.add_row(["Temperature", 12.5])
dataset.add_row(["Humidity", 50.0])
node.add_data_metric("Properties/Measurements", DataType.DataSet, dataset)

node.add_command_metric("Node Control/Print", DataType.String, "", lambda value: print(value))
```

Finally, start a coroutine using `asyncio` to run the Sparkplug B Edge Node:

```python
async def main():
    node.connect()
    
    while True:
        await asyncio.sleep(0.5)
        temperature = get_temp_measurement() # Determine the current value of your metric(s)
        node.set_metric_value("Properties/Temperature", temperature)


asyncio.run(main())
```

For some tips on how to structure your code and how to use `asyncio`, see [this unofficial asyncio tutorial](https://github.com/peterhinch/micropython-async/blob/master/v3/docs/TUTORIAL.md).

## API

### <code><i>class</i> sparkplugb.payload.DataType</code>

This class is used to define the datatype of a metric. It has several class attributes that represent the different datatypes supported by the Sparkplug B specification. These can be used in many places in the library where a datatype is required.

#### Usage
```python
from sparkplugb.payload import DataType

float_type = DataType.Float
string_type = DataType.String
dataset_type = DataType.DataSet
```

### <code><i>class</i> sparkplugb.payload.DataSet(<i>columns: list[str], types: list</i>)</code>

Objects of this class can be used as a value for DataSet metrics. (See [example](#usage-example).)
`columns` is a list of strings representing the names of the columns in the DataSet. `types` is a list of numbers representing the datatypes of the columns in the DataSet. For defining the datatypes, use the class attributes of `DataType`.

#### <code>DataSet.add_row(<i>elements: list</i>)</code>

Adds a row to the DataSet. That row contains the elements in `elements` which should be a list of the same length as the number of columns in the DataSet.


### <code><i>class</i> sparkplugb.edge_node.SparkplugBEdgeNode(<i>mqtt_client, group_id: str, edge_node_id: str, primary_host_application_id: str = None, bdSeq_file: str = "bdSeq", debug: bool = False</i>)</code>

This class represents a Sparkplug B Edge Node. It is used to create and manage the Edge Node and its metrics. The class has several methods for starting and stopping a connection, adding and removing metrics and setting metric values.

The `mqtt_client` given must have the same API as the clients defined in the modules `umqtt.simple` or `umqtt.robust` (see [Choosing an MQTT Client](#choosing-an-mqtt-client)).

The `group_id` and `edge_node_id` are strings that identify the Edge Node and its group. They will be used in the topics that the node publishes to. The `primary_host_application_id` is optional and defines a primary host application that the Edge Node will wait for to be online before sending Sparkplug messages. (See the Sparkplug B Specification for more information on primary host applications.)

The `bdSeq_file` is the name of the file where the Edge Node will store its current `bdSeq` value. This file is used to persist the birth/death sequence number of the Edge Node even between device reboots. 

If `debug` is set to `True`, the Edge Node will print some debug messages to the console.

#### `SparkplugBEdgeNode.connect()`

Connects the Edge Node to the MQTT broker using the configured MQTT client and subscribes to the topics that the Edge Node should listen to. Also sets up message handling and keepalive/ping messages. 

If a primary host application is defined, the Edge Node will wait for the primary host application to be online before sending Sparkplug messages. The Edge Node will send a NBIRTH message when the primary host application is online.

#### `SparkplugBEdgeNode.disconnect()`

Terminates the current Sparkplug B session if it is active and disconnects the Edge Node from the MQTT broker.

#### <code>SparkplugBEdgeNode.add_data_metric(<i>name, datatype, value</i>)</code>

Adds a data metric to the Edge Node with the given `name`and `datatype`. The `value` is the initial value of the metric. For DataSet metrics, the `value` should be an instance of the `DataSet` class.

Metrics can be added after the Edge Node has been connected. In that case, if a Sparkplug Session is active, the changed metric list will be republished with a new NBIRTH message.

#### <code>SparkplugBEdgeNode.add_command_metric(<i>name, datatype, value, callback</i>)</code>

Adds a command metric to the Edge Node with the given `name`, `datatype`, and initial `value`. The `callback` is a function that will be called when a command (NCMD message) is received for this metric. The function should take one argument, which is the value of the command metric sent in the NCMD.

Metrics can be added after the Edge Node has been connected. In that case, if a Sparkplug Session is active, the changed metric list will be republished with a new NBIRTH message.

#### <code>SparkplugBEdgeNode.remove_metric(<i>name</i>)</code>

Removes the metric with the given `name` from the Edge Node.

Metrics can be removed after the Edge Node has been connected. In that case, if a Sparkplug Session is active, the changed metric list will be republished with a new NBIRTH message.

#### <code>SparkplugBEdgeNode.set_metric_value(<i>name, value</i>)</code>

Sets the value of the metric with the given `name` to the given `value`. The `value` should be of the correct datatype for the metric. If the metric is a DataSet metric, the `value` should be an instance of the `DataSet` class.

To comply with the Report-By-Exception (RBE) principle of the Sparkplug B specification, the Edge Node will only send an NDATA message if the new value is different from the previous value.

## Notes

### Limitations

Some features of the Sparkplug B specification are not (yet) implemented in this library. These include:
- Some datatypes/properties for metrics: The Protobuf schema from the Sparkplug B Specification is not yet fully implemented, but the most common datatypes are and completing the schema would be fairly straightforward (feel free to open an issue or a pull request). The following is still missing: Metadata and Properties for metrics, template metrics, array metric values.
- Devices (with their own metrics) that can be assigned to an Edge Node, currently all metrics are assumed to be part of the Edge Node and their values are sent in NBIRTH/NDATA messages (no support for DBIRTH/DDEATH/DDATA messages)
- Connecting to a cluster of MQTT brokers

### Dependencies

This library depends on the following libraries:
- [`minipb`](https://github.com/dogtopus/minipb) for encoding and decoding Sparkplug B messages in Protocol Buffers format
- `logging` (dependency of `minipb`)
- `bisect` (dependency of `minipb`)

### Testing

This library has been tested on the ESP32 port and the Unix port of MicroPython. It should work on other ports as well, but this has not been tested.

The tests were done using the Sparkplug TCK (Test Compatibility Kit). The tests were run on the ESP32 port of MicroPython using the `umqtt.simple` client.

### Contributing

Contributions are welcome! Please open an issue or a pull request if you have any suggestions or improvements.


## License

This project is licensed under the [MIT License](LICENSE).
