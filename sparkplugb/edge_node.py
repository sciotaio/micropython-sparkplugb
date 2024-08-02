# (C) Copyright sciota GmbH 2024.
# Released under the MIT license.

from . import payload as spb
import asyncio

class SparkplugBEdgeNode:
    def __init__(self, mqtt_client, group_id: str, edge_node_id: str, primary_host_application_id: str = None, bdSeq_file: str = "bdSeq", debug: bool = False):
        self.debug = debug
        self.client = mqtt_client
        self.connected = False
        self.group_id = group_id
        self.edge_node_id = edge_node_id
        self.primary_host_application_id = primary_host_application_id
        self.previous_primary_host_application_msg = None # The last message received from the primary host application - a tuple consisting of the timestamp and the online state
        self.sparkplug_session_established = False
        self.bdSeq_file = bdSeq_file
        self.bdSeq = self._load_bdSeq()
        self.seq = 0
        self.metrics = { "Node Control/Rebirth": (spb.DataType.Boolean, False) } # A dictionary of metric names to tuples of datatype and value
        self.commands = {
            "Node Control/Rebirth": lambda value: self._publish_nbirth() if value and self.sparkplug_session_established else None
        }
        
    def _debug(self, msg):
        if self.debug:
            print(msg)
        
    def _load_bdSeq(self):
        try:
            with open(self.bdSeq_file, "r") as f:
                bdSeq = int(f.read())
                self._debug(f"Loaded bdSeq {bdSeq}")
                return bdSeq
        except:
            self._debug(f"Could not load bdSeq. Setting to 0.")
            return 0
        
    def _save_bdSeq(self):
        with open(self.bdSeq_file, "w") as f:
            f.write(str(self.bdSeq))

    def _increment_bdSeq(self):
        self.bdSeq = (self.bdSeq + 1) % 256

    def _increment_seq(self):
        self.seq = (self.seq + 1) % 256

    def _get_nbirth_topic(self):
        return f"spBv1.0/{self.group_id}/NBIRTH/{self.edge_node_id}"

    def _get_ndeath_topic(self):
        return f"spBv1.0/{self.group_id}/NDEATH/{self.edge_node_id}"

    def _get_ndata_topic(self):
        return f"spBv1.0/{self.group_id}/NDATA/{self.edge_node_id}"

    def _get_ncmd_topic(self):
        return f"spBv1.0/{self.group_id}/NCMD/{self.edge_node_id}"
    
    def _get_primary_host_application_state_topic(self):
        return f"spBv1.0/STATE/{self.primary_host_application_id}"

    def _get_ndeath_payload(self):
        payload = spb._Payload(use_timestamp=False)
        payload.add_metric("bdSeq", spb.DataType.Int64, self.bdSeq)
        return spb._encode_payload(payload)

    def _get_nbirth_payload(self):
        payload = spb._Payload(seq=self.seq)
        payload.add_metric("bdSeq", spb.DataType.Int64, self.bdSeq)

        for name, (datatype, value) in self.metrics.items():
            payload.add_metric(name, datatype, value)

        return spb._encode_payload(payload)

    def _get_ndata_payload(self, changed_metrics: list[str]):
        payload = spb._Payload(seq=self.seq)

        for name in changed_metrics:
            datatype, value = self.metrics[name]
            payload.add_metric(name, datatype, value)

        return spb._encode_payload(payload)

    def _publish_nbirth(self):
        self.client.publish(self._get_nbirth_topic(), self._get_nbirth_payload(), qos=0, retain=False)
        self._increment_seq()

    def _publish_ndeath(self):
        self.client.publish(self._get_ndeath_topic(), self._get_ndeath_payload(), qos=1, retain=False)

    def _publish_ndata(self, changed_metrics: list[str]):
        self.client.publish(self._get_ndata_topic(), self._get_ndata_payload(changed_metrics), qos=0, retain=False)
        self._increment_seq()
        
    def _establish_sparkplug_session(self):
        self._debug("Establishing Sparkplug session")
        self.sparkplug_session_established = True
        self._publish_nbirth()
        
    def _terminate_sparkplug_session(self):
        self._debug("Terminating Sparkplug session")
        self.sparkplug_session_established = False
        self._publish_ndeath()
        
    def _handle_ncmd_msg(self, msg: bytes):
        payload = spb._decode_payload(msg)
        for metric in payload['metrics']:
            name = metric['name']
            value = spb._get_metric_value(metric, self.metrics[name][0] if name in self.metrics else None)
            self._debug(f"Executing command {name} with value {value}")
            if name in self.commands:
                self.commands[name](value)
            else:
                self._debug(f"Command {name} does not exist. Not executing.")
                
    def _handle_primary_host_application_state_msg(self, msg: bytes):
        payload = spb._decode_host_application_state_payload(msg)
        timestamp = payload['timestamp']
        online = payload['online']
        self._debug(f"Received primary host application state message: online={online}, timestamp={timestamp}")
        if self.previous_primary_host_application_msg is None or timestamp >= self.previous_primary_host_application_msg[0]:
            self.previous_primary_host_application_msg = (timestamp, online)
            if online and not self.sparkplug_session_established:
                self._establish_sparkplug_session()
            elif not online and self.sparkplug_session_established:
                self._terminate_sparkplug_session()
        
    def _handle_msg(self, topic: bytes, msg: bytes):
        try:
            topic = topic.decode('utf-8') # SparkplugB topics are always UTF-8 encoded
            self._debug(f'Received message {msg} on topic {topic}')
            
            if topic == self._get_ncmd_topic():
                self._handle_ncmd_msg(msg)
            elif topic == self._get_primary_host_application_state_topic():
                self._handle_primary_host_application_state_msg(msg)
                
        except Exception as e:
            print(f"Error while handling message: {e}")

    async def _check_msg(self):
        while True:
            await asyncio.sleep(0.2)
            while self.client.check_msg() is not None:
                # check for available messages until there are none left
                await asyncio.sleep(0)
                
    async def _keep_alive(self, interval: int):
        while True:
            await asyncio.sleep(interval / 2)
            if self.connected:
                self.client.ping()
                
    def _are_values_equal(self, value1, value2):
        if value1 == value2:
            return True
        if type(value1) != type(value2):
            return False
        if isinstance(value1, float):
            return abs(value1 - value2) < 0.00001
        return False

    def connect(self):
        self._increment_bdSeq()
        self.client.set_last_will(self._get_ndeath_topic(), self._get_ndeath_payload(), qos=1, retain=False)
        self._debug(f"Last Will set for {self._get_ndeath_topic()}")

        self.client.set_callback(self._handle_msg)

        self.client.connect()
        self.connected = True
        self._save_bdSeq() # Connected successfully so save the new (incremented) bdSeq for next time
        print(f"Connected to MQTT Broker {self.client.server}")
        
        asyncio.create_task(self._check_msg())
        
        if self.client.keepalive > 0:
            asyncio.create_task(self._keep_alive(self.client.keepalive))
        
        sub_topic = self._get_ncmd_topic()
        self.client.subscribe(sub_topic, qos=1)
        self._debug(f"Subscribed to {sub_topic}")
        
        if self.primary_host_application_id is not None:
            # Need to subscribe to the primary host application state topic and wait for it to be online
            primary_host_application_state_topic = self._get_primary_host_application_state_topic()
            self.client.subscribe(primary_host_application_state_topic, qos=1)
            self._debug(f"Subscribed to {primary_host_application_state_topic}")
        else:
            self._establish_sparkplug_session()

    def disconnect(self):
        if self.sparkplug_session_established:
            self._terminate_sparkplug_session()
        self.client.disconnect()
        self._debug("Disconnected from MQTT Broker")
        self.connected = False

    def set_metric_value(self, name, value):
        if name not in self.metrics:
            print(f"Metric {name} does not exist. Not setting value.")
            return
        
        if self._are_values_equal(value, self.metrics[name][1]):
            # The metric value did not change so no need to update (Report by exception)
            return
        
        self.metrics[name] = (self.metrics[name][0], value)

        if self.sparkplug_session_established:
            self._publish_ndata([name])

    def add_data_metric(self, name, datatype, value):
        if name in self.metrics:
            print(f"Metric {name} already exists. Not adding.")
            return
        self.metrics[name] = (datatype, value)

        if self.sparkplug_session_established:
            # Rebirth needed because the available metrics changed
            self._debug(f"Publishing rebirth because metric {name} was added")
            self._publish_nbirth()

    def add_command_metric(self, name, datatype, value, callback):
        self.commands[name] = callback
        self.add_data_metric(name, datatype, value)

    def remove_metric(self, name):
        if name not in self.metrics:
            print(f"Metric {name} does not exist. Not removing.")
            return
        del self.metrics[name]

        if name in self.commands:
            del self.commands[name]

        if self.sparkplug_session_established:
            # Rebirth needed because the available metrics changed
            self._debug(f"Publishing rebirth because metric {name} was removed")
            self._publish_nbirth()
