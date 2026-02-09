"""
Sony Camera PTP-IP Client - Main Client Class
"""

import time
import threading
import logging
import random
import struct
import asyncio
from threading import Timer
from typing import Optional

from .protocol import (
    PTP_STATES, PTPIP_INIT_COMMAND_ACK, PTPIP_INIT_EVENT_ACK, PTPIP_OPER_RESP,
    PTPIP_PROBE_REQ, PTPIP_PROBE_RESP, PTPIP_START_DATA, PTPIP_DATA, PTPIP_END_DATA,
    PTPIP_EVENT, PTP_OPERATIONS, PTP_RESPONSES, SONY_PROPERTIES,
    create_init_cmd_req_packet, create_init_event_req_packet, create_operation_request,
    create_probe_resp, create_probe_req, parse_packet_header, parse_init_cmd_ack,
    parse_operation_response
)
from .connection import ConnectionManager
from .zoom import ZoomController

class SonyCamera:
    """Sony Camera PTP-IP client with task loop"""
    
    def __init__(self, ip_address: str, ssh_username: str = None, ssh_password: str = None):
        self.ip_address = ip_address
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        
        # Connection management
        self.connection = ConnectionManager(ip_address, ssh_username, ssh_password)
        
        # Connection state
        self.connected = False
        self.state = PTP_STATES.INIT
        
        # Session info
        self.session_id = None
        self.connection_id = None
        self.transaction_id = 1
        self.guid = self._generate_guid()
        
        # Camera info
        self.camera_name = ''
        
        # Incoming stream buffers for packet framing
        self._cmd_buffer = b''
        self._event_buffer = b''
        self._inbound_data = None
        
        # Task loop
        self.task_timer = None
        self.task_running = False
        
        # Event handlers
        self._event_handlers = {}
        
        # SDIO readiness
        self.sdio_ready = False
        self._sdio_ready_promise = None
        
        # Keepalive
        self.keepalive_timer = None
        self.keepalive_interval = 30.0
        self.last_activity = time.time()
        
        # Zoom controller
        self.zoom = ZoomController(self)
    
    def _generate_guid(self) -> bytes:
        """Generate a 16-byte GUID for camera pairing"""
        return bytes([random.randint(0, 255) for _ in range(16)])
    
    # Event System
    def emit(self, event: str, *args):
        """Emit event to handlers"""
        if event in self._event_handlers:
            for handler in self._event_handlers[event][:]:
                try:
                    handler(*args)
                except Exception as e:
                    logging.debug(f"Event handler error: {e}")
    
    def on(self, event: str, handler):
        """Add event handler"""
        if event not in self._event_handlers:
            self._event_handlers[event] = []
        self._event_handlers[event].append(handler)
    
    def once(self, event: str, handler):
        """Add one-time event handler"""
        def wrapper(*args):
            handler(*args)
            if event in self._event_handlers and wrapper in self._event_handlers[event]:
                self._event_handlers[event].remove(wrapper)
        self.on(event, wrapper)
    
    # Connection Management
    async def connect(self) -> bool:
        """Connect to camera"""
        if self.connected:
            return True
        
        #logging.info('🔗 Connecting to Sony Alpha camera...')
        
        try:
            # Establish connection (SSH tunnel + sockets)
            await self.connection.establish_connection()
            
            # Set up data handlers
            self._setup_data_handlers()
            
            # Start handshake with grace period
            Timer(0.15, self._start_handshake).start()
            
            return True
            
        except Exception as e:
            logging.error(f'❌ Connection failed: {e}')
            await self.disconnect()
            return False
    
    def _setup_data_handlers(self):
        """Set up data handlers for both channels"""
        # Command channel handler
        def handle_command_data():
            try:
                while True:
                    data = self.connection.command_socket.recv(4096)
                    if not data:
                        break
                    
                    self._cmd_buffer += data
                    while len(self._cmd_buffer) >= 8:
                        pkt_len, _ = parse_packet_header(self._cmd_buffer)
                        if not pkt_len or pkt_len > 1_000_000:
                            self._cmd_buffer = b''
                            break
                        if len(self._cmd_buffer) < pkt_len:
                            break
                        
                        pkt = self._cmd_buffer[:pkt_len]
                        self._cmd_buffer = self._cmd_buffer[pkt_len:]
                        self._handle_command_data(pkt)
                        
            except Exception as e:
                logging.debug(f"Command data handler error: {e}")
        
        # Event channel handler
        def handle_event_data():
            try:
                while True:
                    data = self.connection.event_socket.recv(4096)
                    if not data:
                        break
                    
                    self._event_buffer += data
                    while len(self._event_buffer) >= 8:
                        pkt_len, _ = parse_packet_header(self._event_buffer)
                        if not pkt_len or pkt_len > 1_000_000:
                            self._event_buffer = b''
                            break
                        if len(self._event_buffer) < pkt_len:
                            break
                        
                        pkt = self._event_buffer[:pkt_len]
                        self._event_buffer = self._event_buffer[pkt_len:]
                        self._handle_event_data(pkt)
                        
            except Exception as e:
                logging.debug(f"Event data handler error: {e}")
        
        threading.Thread(target=handle_command_data, daemon=True).start()
        threading.Thread(target=handle_event_data, daemon=True).start()
    
    # Handshake & Task Loop
    def _start_handshake(self):
        """Start the handshake sequence with task loop"""
        logging.info('🤝 Starting PTP-IP handshake...')
        
        tick = 0.04 + random.random() * 0.02  # 40-60ms
        self.state = PTP_STATES.CMD_REQ
        self._start_task_loop(tick)
        
        self.once('ready', self._on_handshake_complete)
        self.once('handshakeFailed', self._on_handshake_failed)
    
    def _start_task_loop(self, interval: float):
        """Start the task loop"""
        if self.task_running:
            return
            
        self.task_running = True
        
        def run_task():
            if self.task_running:
                self.task()
                self.task_timer = Timer(interval, run_task)
                self.task_timer.start()
        
        run_task()
    
    def _stop_task_loop(self):
        """Stop the task loop"""
        self.task_running = False
        if self.task_timer:
            self.task_timer.cancel()
            self.task_timer = None
    
    def task(self):
        """Task loop - handles state machine progression"""
        try:
            if self.state == PTP_STATES.CMD_REQ:
                cmd_req_packet = create_init_cmd_req_packet(self.guid)
                self.connection.command_socket.send(cmd_req_packet)
                logging.info('📤 Sent INIT_CMD_REQ')
                self.state = PTP_STATES.CMD_REQ + 1
                
            elif self.state == PTP_STATES.EVENT_REQ:
                if not self.connection_id:
                    return
                
                event_req_packet = create_init_event_req_packet(self.connection_id)
                self.connection.event_socket.send(event_req_packet)
                logging.info('📤 Sent INIT_EVENT_REQ')
                self.state = PTP_STATES.EVENT_REQ + 1
                
            elif self.state == PTP_STATES.OPEN_SESSION:
                self.session_id = random.randint(1, 0xFFFFFF)
                session_packet = create_operation_request(
                    PTP_OPERATIONS['OPEN_SESSION'], 
                    self.transaction_id, 
                    [self.session_id]
                )
                self.transaction_id += 1
                self.connection.command_socket.send(session_packet)
                logging.info(f'📤 OpenSession (ID: {self.session_id})')
                self.state = PTP_STATES.OPEN_SESSION + 1
                
        except Exception as e:
            logging.error(f"Task loop error: {e}")
            self.emit('handshakeFailed', e)
    
    # Data Handlers
    def _handle_command_data(self, data: bytes):
        """Handle command channel data"""
        if len(data) < 8:
            return
        
        self.last_activity = time.time()
        
        _, packet_type = parse_packet_header(data)
        
        if packet_type == PTPIP_INIT_COMMAND_ACK:
            self._handle_init_cmd_ack(data)
        elif packet_type == PTPIP_OPER_RESP:
            self._handle_operation_response(data)
        elif packet_type == PTPIP_PROBE_REQ:
            self._send_probe_resp(self.connection.command_socket)
        elif packet_type == PTPIP_PROBE_RESP:
            logging.debug('💓 Received keepalive PROBE_RESP')
        elif packet_type == PTPIP_START_DATA:
            if len(data) >= 16:
                tx_id = int.from_bytes(data[8:12], 'little')
                pending_len = int.from_bytes(data[12:16], 'little')
                self._inbound_data = {'tx_id': tx_id, 'pending_len': pending_len, 'chunks': []}
        elif packet_type == PTPIP_DATA:
            if len(data) >= 12:
                tx_id = int.from_bytes(data[8:12], 'little')
                payload = data[12:]
                if self._inbound_data and self._inbound_data['tx_id'] == tx_id:
                    self._inbound_data['chunks'].append(payload)
        elif packet_type == PTPIP_END_DATA:
            if len(data) >= 12:
                tx_id = int.from_bytes(data[8:12], 'little')
                payload = b''
                if self._inbound_data and self._inbound_data['tx_id'] == tx_id:
                    payload = b''.join(self._inbound_data['chunks'])
                self.emit('operationData', tx_id, payload)
                self._inbound_data = None
    
    def _handle_event_data(self, data: bytes):
        """Handle event channel data"""
        if len(data) < 8:
            return
        
        self.last_activity = time.time()
        
        _, packet_type = parse_packet_header(data)
        
        if packet_type == PTPIP_INIT_EVENT_ACK:
            self._handle_init_event_ack(data)
        elif packet_type == PTPIP_PROBE_REQ:
            self._send_probe_resp(self.connection.event_socket)
        elif packet_type == PTPIP_PROBE_RESP:
            logging.debug('💓 Received keepalive PROBE_RESP on event channel')
        elif packet_type == PTPIP_EVENT:
            if len(data) >= 10:
                event_code = int.from_bytes(data[8:10], 'little')
                self.emit('cameraEvent', event_code, data)
    
    def _handle_init_cmd_ack(self, data: bytes):
        """Handle INIT_CMD_ACK"""
        logging.info('✅ Pairing accepted!')
        
        ack_info = parse_init_cmd_ack(data)
        if 'connection_id' in ack_info:
            self.connection_id = ack_info['connection_id']
        
        if 'camera_name' in ack_info:
            self.camera_name = ack_info['camera_name']
            logging.info(f'📷 Camera: {self.camera_name}')
        
        self.state = PTP_STATES.EVENT_REQ
    
    def _handle_init_event_ack(self, data: bytes):
        """Handle INIT_EVENT_ACK"""
        logging.info('✅ Event channel ready')
        self.state = PTP_STATES.OPEN_SESSION
    
    def _handle_operation_response(self, data: bytes):
        """Handle operation response"""
        resp_info = parse_operation_response(data)
        response_code = resp_info.get('response_code')
        
        if self.state in [PTP_STATES.OPEN_SESSION, PTP_STATES.OPEN_SESSION + 1]:
            if response_code == PTP_RESPONSES['OK']:
                if self.state == PTP_STATES.OPEN_SESSION:  # Only log once
                    logging.info('✅ Session opened successfully!')
                self.connected = True
                self.state = PTP_STATES.READY
                self._stop_task_loop()
                
                # Start SDIO establishment
                if not self.sdio_ready and not self._sdio_ready_promise:
                    self._establish_sdio_connection()
                
                self.emit('ready')
            elif response_code == PTP_RESPONSES['SESSION_ALREADY_OPEN']:
                logging.debug('Session already open (0x2013) - continuing...')
            else:
                logging.error(f'❌ Session failed: 0x{response_code:04x}')
                self.emit('handshakeFailed', Exception(f'Session failed: 0x{response_code:04x}'))
    
    # SDIO Connection
    def _establish_sdio_connection(self):
        """Establish SDIO connection"""
        if self._sdio_ready_promise:
            return
            
        self._sdio_ready_promise = True
        logging.info('🔧 Establishing SDIO connection...')
        
        def run_sdio_sequence():
            try:
                def send_operation_sync(op_code, params=None):
                    if params is None:
                        params = []
                    packet = create_operation_request(op_code, self.transaction_id, params)
                    self.transaction_id += 1
                    self.connection.command_socket.send(packet)
                    time.sleep(0.1)
                    return True
                
                # SDIO sequence
                send_operation_sync(PTP_OPERATIONS['GET_DEVICE_INFO'], [])
                time.sleep(0.05)
                send_operation_sync(PTP_OPERATIONS['GET_STORAGE_IDS'], [])
                time.sleep(0.05)
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [1, 0, 0])
                time.sleep(0.05)
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [2, 0, 0])
                time.sleep(0.05)
                send_operation_sync(PTP_OPERATIONS['SDIO_GET_EXT_DEVICE_INFO'], [0x012C, 0, 0])
                time.sleep(0.05)
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [3, 0, 0])
                time.sleep(0.08)
                send_operation_sync(PTP_OPERATIONS['SDIO_GET_EXT_DEVICE_INFO'], [0x012C, 0, 0])
                
                logging.info('✅ SDIO sequence complete')
                self.sdio_ready = True
                
                try:
                    # Get all device properties - this also initializes our recording state cache
                    response = send_operation_sync(PTP_OPERATIONS['SONY_GET_ALL_DEVICE_PROP_DATA'], [])
                    
                    # Try to extract initial recording state from the properties
                    if response and len(response) > 100:
                        try:
                            recording_prop = SONY_PROPERTIES['RECORDING_STATE']  # 0xD21D
                            recording_bytes = struct.pack('<H', recording_prop)
                            
                            # Search for recording state property
                            for offset in range(0, len(response) - 16):
                                if response[offset:offset+2] == recording_bytes:
                                    # Found it! Extract the value (offset +8)
                                    value_offset = offset + 8
                                    if value_offset + 4 <= len(response):
                                        rec_state = struct.unpack('<I', response[value_offset:value_offset+4])[0]
                                        # Cache the initial recording state (emit event for managers to catch)
                                        is_recording = (rec_state == 1)
                                        logging.info(f"📹 Initial recording state: {'Recording' if is_recording else 'Stopped'}")
                                        self.emit('recording_state_detected', is_recording)
                                    break
                        except Exception as e:
                            logging.debug(f"Could not extract initial recording state: {e}")
                    
                    logging.info('✅ Camera fully ready!')
                except Exception as e:
                    logging.warning(f'Device properties failed: {e}')
                
            except Exception as e:
                logging.error(f'SDIO sequence failed: {e}')
                self.sdio_ready = False
        
        threading.Thread(target=run_sdio_sequence, daemon=True).start()
    
    async def ensure_sdio_ready(self):
        """Ensure SDIO ready before vendor control ops"""
        if self.sdio_ready:
            return True
        
        if self._sdio_ready_promise:
            time.sleep(2)
            return self.sdio_ready
        
        self._sdio_ready_promise = True
        self._establish_sdio_connection()
        time.sleep(3)
        return self.sdio_ready
    
    # Keepalive
    def _on_handshake_complete(self):
        """Handshake completion handler"""
        logging.info('🎉 PTP-IP handshake completed!')
        self._start_keepalive()
    
    def _on_handshake_failed(self, error):
        """Handshake failure handler"""
        logging.error(f'❌ Handshake failed: {error}')
    
    def _start_keepalive(self):
        """Start keepalive timer"""
        if self.keepalive_timer:
            return
        
        def keepalive_task():
            if not self.connected:
                return
            
            try:
                now = time.time()
                if now - self.last_activity > self.keepalive_interval:
                    self._send_probe_req()
                
                if self.connected:
                    self.keepalive_timer = Timer(self.keepalive_interval, keepalive_task)
                    self.keepalive_timer.start()
                    
            except Exception as e:
                logging.debug(f'Keepalive error: {e}')
        
        self.keepalive_timer = Timer(self.keepalive_interval, keepalive_task)
        self.keepalive_timer.start()
    
    def _stop_keepalive(self):
        """Stop keepalive timer"""
        if self.keepalive_timer:
            self.keepalive_timer.cancel()
            self.keepalive_timer = None
    
    def _send_probe_req(self):
        """Send PROBE_REQ keepalive"""
        try:
            pkt = create_probe_req()
            if self.connection.command_socket:
                self.connection.command_socket.send(pkt)
            if self.connection.event_socket:
                self.connection.event_socket.send(pkt)
        except Exception as e:
            logging.debug(f'Failed sending PROBE_REQ: {e}')
    
    def _send_probe_resp(self, sock):
        """Send PROBE_RESP"""
        try:
            pkt = create_probe_resp()
            sock.send(pkt)
        except Exception as e:
            logging.debug(f'Failed sending PROBE_RESP: {e}')
    
    # Public Interface
    def is_ready(self) -> bool:
        """Check if camera is ready"""
        return self.connected and self.state == PTP_STATES.READY
    
    async def start_zoom(self, direction: str, speed: int = 1):
        """Start continuous zoom with speed"""
        return await self.zoom.start_zoom(direction, speed)
    
    async def stop_zoom(self):
        """Stop continuous zoom"""
        return await self.zoom.stop_zoom()
    
    async def _send_operation_and_wait(self, op_code: int, params: bytes = None, timeout: float = 2.0):
        """Send PTP operation and wait for response"""
        if not self.connection.command_socket:
            raise Exception('Command socket not available')
        
        # Get current transaction ID and increment
        tx_id = self.transaction_id
        self.transaction_id += 1
        
        # Create operation request packet
        if params:
            if len(params) == 2:  # Single 16-bit parameter
                param_list = [struct.unpack('<H', params)[0]]
                logging.info(f"🔧 Single param: 0x{param_list[0]:04X}")
            elif len(params) == 8:  # Two 32-bit parameters
                param1, param2 = struct.unpack('<II', params)
                param_list = [param1, param2]
                logging.info(f"🔧 Two params: 0x{param1:08X}, 0x{param2:08X}")
            else:
                param_list = []
                logging.info(f"🔧 Unknown param length: {len(params)}")
        else:
            param_list = []
            logging.info("🔧 No parameters")
            
        packet = create_operation_request(op_code, tx_id, param_list)
        #logging.info(f"🔧 Created packet: {len(packet)} bytes, tx_id={tx_id}")
        
        try:
            # Send operation request
            self.connection.command_socket.send(packet)
            #logging.info(f"🔧 Sent operation request")
            
            # Wait for response with timeout
            start_time = time.time()
            response_count = 0
            while time.time() - start_time < timeout:
                try:
                    data = self.connection.command_socket.recv(4096)
                    if data:
                        response_count += 1
                        #logging.info(f"🔧 Received response #{response_count}: {len(data)} bytes")
                        
                        # Parse response - look for operation response
                        try:
                            length, pkt_type = parse_packet_header(data)
                            payload = data[8:]  # Payload starts after 8-byte header
                            #logging.info(f"🔧 Packet type: 0x{pkt_type:08X} (expecting 0x{PTPIP_OPER_RESP:08X}), length: {length}, payload: {len(payload)} bytes")
                        except Exception as parse_error:
                            logging.error(f"🔧 Failed to parse packet header: {parse_error}")
                            #logging.info(f"🔧 Raw packet data: {data[:50].hex()}...")
                            continue
                        
                        if pkt_type == PTPIP_OPER_RESP:
                            #logging.info(f"🔧 Found operation response!")
                            # Extract response data (skip header)
                            if len(payload) > 8:  # Response code + transaction ID
                                result = payload[8:]
                                #logging.info(f"🔧 Returning {len(result)} bytes of data")
                                return result
                            else:
                                logging.info(f"🔧 Empty response payload")
                                return b''
                        elif pkt_type == PTPIP_START_DATA:
                            logging.info(f"🔧 Got START_DATA packet - collecting all data...")
                            # START_DATA is just the beginning, we need to collect more packets
                            all_data = b''
                            
                            # Add initial data from START_DATA
                            if len(payload) >= 12:
                                all_data += payload[12:]
                                #logging.info(f"🔧 START_DATA initial data: {len(payload[12:])} bytes")
                            elif len(payload) >= 8:
                                all_data += payload[8:]
                                #logging.info(f"🔧 START_DATA initial data: {len(payload[8:])} bytes")
                            else:
                                all_data += payload
                                #logging.info(f"🔧 START_DATA initial data: {len(payload)} bytes")
                            
                            # Continue collecting until we have substantial data or timeout
                            while time.time() - start_time < timeout and len(all_data) < 1000:
                                try:
                                    more_data = self.connection.command_socket.recv(4096)
                                    if more_data:
                                        response_count += 1
                                        #logging.info(f"🔧 Received response #{response_count}: {len(more_data)} bytes")
                                        
                                        # Try to parse the packet
                                        try:
                                            more_length, more_pkt_type = parse_packet_header(more_data)
                                            more_payload = more_data[8:]
                                            #logging.info(f"🔧 Additional packet type: 0x{more_pkt_type:08X}, payload: {len(more_payload)} bytes")
                                            
                                            # Collect data from any packet type that has substantial payload
                                            if len(more_payload) > 100:  # Only collect substantial data
                                                all_data += more_payload
                                                #logging.info(f"🔧 Added {len(more_payload)} bytes, total: {len(all_data)} bytes")
                                                
                                        except:
                                            # If we can't parse the header, just use the raw data
                                            if len(more_data) > 100:
                                                all_data += more_data[8:]  # Skip potential header
                                                #logging.info(f"🔧 Added raw data: {len(more_data[8:])} bytes, total: {len(all_data)} bytes")
                                    
                                except Exception as e:
                                    logging.debug(f"🔧 Error collecting additional data: {e}")
                                    break
                                    
                                await asyncio.sleep(0.01)
                            
                            #logging.info(f"🔧 Final collected data: {len(all_data)} bytes")
                            return all_data if len(all_data) > 10 else None
                        elif pkt_type == PTPIP_DATA:
                            #logging.info(f"🔧 Got standalone DATA packet - collecting data")
                            result = payload
                            #logging.info(f"🔧 Using DATA payload: {len(result)} bytes")
                            return result
                        else:
                            logging.info(f"🔧 Unknown packet type, continuing...")
                except Exception as e:
                    logging.debug(f"🔧 Socket recv error: {e}")
                    pass
                await asyncio.sleep(0.01)
            
            logging.info(f"🔧 Timeout after {timeout}s, received {response_count} responses")
            return None
            
        except Exception as e:
            logging.error(f'Failed to send operation 0x{op_code:04X}: {e}')
            raise
    
    async def get_zoom_bar_info(self):
        """Get current zoom bar information"""
        try:
            if not self.is_ready():
                logging.info("Camera not ready for zoom bar query")
                return None
            
            logging.info("🔍 Querying zoom bar info...")
            
            # Use SDIO_GetAllExtDevicePropInfo to get all device properties
            op_code = PTP_OPERATIONS['SONY_GET_ALL_DEVICE_PROP_DATA']  # 0x9209
            # Parameter 1: Flag of get only difference data (0x00000000 = get all data)
            # Parameter 2: Flag of Device Property Option (0x00000001 = enable extended properties)
            params = struct.pack('<II', 0x00000000, 0x00000001)
            
            #logging.info(f"📡 Sending all device properties query: op_code=0x{op_code:04X} with extended properties flag")
            
            response_data = await self._send_operation_and_wait(op_code, params)
            
            #logging.info(f"📊 Zoom bar response: {response_data.hex() if response_data else 'None'} (length: {len(response_data) if response_data else 0})")
            
            if response_data and len(response_data) >= 8:
                # Log basic info about the response
                #logging.info(f"📋 Got device properties data: {len(response_data)} bytes")
                
                try:
                    # The Sony format might not start with a count - let's search for the zoom property directly
                    zoom_property = SONY_PROPERTIES['ZOOM_BAR_INFO']  # 0xD25D
                    
                    # Search for the property code in little-endian format
                    zoom_property_bytes = struct.pack('<H', zoom_property)  # 0x5DD2 in little-endian
                    
                    #logging.info(f"🔍 Searching for zoom property bytes: {zoom_property_bytes.hex()}")
                    
                    # Find the property in the data
                    for offset in range(0, len(response_data) - 10, 2):  # Step by 2 bytes
                        if response_data[offset:offset+2] == zoom_property_bytes:
                            #logging.info(f"🎯 Found zoom property 0x{zoom_property:04X} at offset {offset}")
                            
                            # Try to extract the current value - it should be a few bytes after the property code
                            # Sony format: PropertyCode(2) + DataType(2) + GetSet(1) + IsEnabled(1) + FactoryDefault(var) + CurrentValue(var)
                            
                            # Skip property code (2 bytes) and look for the current value
                            # Try different offsets to find the 4-byte zoom value
                            for value_offset in [6, 8, 10, 12, 14, 16]:
                                if offset + value_offset + 4 <= len(response_data):
                                    zoom_value = struct.unpack('<I', response_data[offset + value_offset:offset + value_offset + 4])[0]
                                    
                                    # Extract components according to Sony documentation
                                    total_boxes = (zoom_value >> 24) & 0xFF      # Bits 31-24
                                    current_box = (zoom_value >> 16) & 0xFF      # Bits 23-16  
                                    position_in_box = zoom_value & 0xFFFF        # Bits 15-0
                                    
                                    # Check if this looks like valid zoom data
                                    if total_boxes > 0 and total_boxes <= 255 and current_box <= total_boxes:
                                        #logging.info(f"🎯 Found valid zoom data at offset {offset + value_offset}: Raw=0x{zoom_value:08X}")
                                        #logging.info(f"🎯 Parsed - Total boxes: {total_boxes}, Current box: {current_box}, Position: {position_in_box}")
                                        
                                        return {
                                            'total_boxes': total_boxes,
                                            'current_box': current_box,
                                            'position_in_box': position_in_box,
                                            'raw_value': zoom_value
                                        }
                            
                            # If we found the property but couldn't parse valid data
                            logging.debug("Found zoom property but couldn't parse valid data")
                            return None

                except Exception as e:
                    logging.debug(f"Error parsing zoom bar data: {e}")
                    return None

            return None

        except Exception as e:
            logging.debug(f"Failed to get zoom bar info: {e}")
            return None