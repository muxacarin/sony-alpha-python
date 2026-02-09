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
    LIVEVIEW_HANDLE,
    WHITE_BALANCE_VALUES, FOCUS_MODE_VALUES, EXPOSURE_PROGRAM_VALUES, SHUTTER_MODE_VALUES,
    TOUCH_FUNCTION_VALUES, ZOOM_SETTING_VALUES, ZOOM_TYPE_STATUS_VALUES,
    format_fnumber, format_shutter_speed, format_shutter_angle, format_iso,
    create_init_cmd_req_packet, create_init_event_req_packet, create_operation_request,
    create_operation_with_data_packets,
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
    async def disconnect(self):
        """Disconnect from camera, close sockets and SSH tunnel."""
        self.connected = False
        self.sdio_ready = False
        self.task_running = False

        if self.keepalive_timer:
            self.keepalive_timer.cancel()
            self.keepalive_timer = None

        if self.task_timer:
            self.task_timer.cancel()
            self.task_timer = None

        await self.connection.disconnect()
        self.emit('disconnected')

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
                    data = self.connection.command_socket.recv(65536)
                    if not data:
                        break
                    
                    self._cmd_buffer += data
                    while len(self._cmd_buffer) >= 8:
                        pkt_len, _ = parse_packet_header(self._cmd_buffer)
                        if not pkt_len or pkt_len > 10_000_000:
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
        tx_id = resp_info.get('transaction_id')
        
        # Emit response event so callers can catch errors
        self.emit('operationResponse', tx_id, response_code)
        
        if response_code and response_code != PTP_RESPONSES['OK']:
            if self.state == PTP_STATES.READY:
                logging.debug(f"PTP response: 0x{response_code:04X} (tx={tx_id})")
        
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
            elif response_code == 0x200F:
                # Access_Denied - camera busy, will retry on next reconnect cycle
                logging.debug('Session Access_Denied (0x200F) - camera busy, will retry')
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
                    time.sleep(0.02)
                    return True
                
                # SDIO sequence
                send_operation_sync(PTP_OPERATIONS['GET_DEVICE_INFO'], [])
                send_operation_sync(PTP_OPERATIONS['GET_STORAGE_IDS'], [])
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [1, 0, 0])
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [2, 0, 0])
                send_operation_sync(PTP_OPERATIONS['SDIO_GET_EXT_DEVICE_INFO'], [0x012C, 0, 0])
                send_operation_sync(PTP_OPERATIONS['SDIO_CONNECT'], [3, 0, 0])
                time.sleep(0.02)
                send_operation_sync(PTP_OPERATIONS['SDIO_GET_EXT_DEVICE_INFO'], [0x012C, 0, 0])
                
                logging.info('✅ SDIO sequence complete')
                self.sdio_ready = True
                
                # Brief pause for pending SDIO responses to clear
                time.sleep(0.1)
                
                # Now read all device properties using the event-based approach
                # to detect initial recording state
                try:
                    self._detect_initial_recording_state()
                    logging.info('✅ Camera fully ready!')
                except Exception as e:
                    logging.warning(f'Initial state detection failed: {e}')
                    logging.info('✅ Camera ready (recording state unknown)')
                
            except Exception as e:
                logging.error(f'SDIO sequence failed: {e}')
                self.sdio_ready = False
        
        threading.Thread(target=run_sdio_sequence, daemon=True).start()
    
    def _detect_initial_recording_state(self):
        """Read bulk device properties via event system to detect if camera is already recording.
        Uses the same approach as get_all_properties_sync() but only looks for RECORDING_STATE.
        """
        tx_id = self.transaction_id
        self.transaction_id += 1

        result_holder = [None]
        done_event = threading.Event()

        def on_data(resp_tx_id, payload):
            if resp_tx_id == tx_id:
                result_holder[0] = payload
                done_event.set()

        def on_response(resp_tx_id, response_code):
            if resp_tx_id == tx_id:
                if result_holder[0] is None:
                    done_event.set()

        self.on('operationData', on_data)
        self.on('operationResponse', on_response)

        try:
            packet = create_operation_request(
                PTP_OPERATIONS['SDIO_GET_ALL_EXT_DEVICE_PROP_INFO'],
                tx_id, []
            )
            self.connection.command_socket.send(packet)
            done_event.wait(timeout=3.0)

            data = result_holder[0]
            if not data or len(data) < 20:
                logging.debug("No bulk property data for recording state detection")
                return

            # Search for RECORDING_STATE (0xD21D) in the bulk data.
            # PTP descriptor layout (from Camera Control PTP 3 Reference):
            #   PropertyCode(2) + DataType(2) + GetSet(1) + IsEnabled(1)
            #   + Reserved(N) + CurrentValue(N)
            # where N = size of the DataType (1 for UINT8, 2 for UINT16, 4 for UINT32).
            # RECORDING_STATE is DataType UINT8 → Reserved=1 byte, CurrentValue at offset 7.
            rec_prop = SONY_PROPERTIES['RECORDING_STATE']
            rec_bytes = struct.pack('<H', rec_prop)
            offset = 0

            while offset < len(data) - 10:
                idx = data.find(rec_bytes, offset)
                if idx == -1:
                    break

                if idx + 8 > len(data):
                    break
                datatype = struct.unpack('<H', data[idx + 2:idx + 4])[0]

                # Reserved field size matches the datatype value size
                dt_sizes = {0x0002: 1, 0x0004: 2, 0x0006: 4}
                val_sz = dt_sizes.get(datatype, 0)
                if val_sz == 0:
                    offset = idx + 2
                    continue

                # CurrentValue offset = 6 + val_sz  (after 2+2+1+1+Reserved(val_sz))
                cv_offset = idx + 6 + val_sz
                if cv_offset + val_sz > len(data):
                    break

                if val_sz == 1:
                    current_value = data[cv_offset]
                elif val_sz == 2:
                    current_value = struct.unpack('<H', data[cv_offset:cv_offset + 2])[0]
                elif val_sz == 4:
                    current_value = struct.unpack('<I', data[cv_offset:cv_offset + 4])[0]
                else:
                    current_value = 0

                is_recording = (current_value == 0x01)
                state_name = {0x00: 'Not Recording', 0x01: 'Recording',
                              0x02: 'Recording Failed', 0x03: 'Waiting Record'}.get(
                    current_value, f'Unknown(0x{current_value:02X})')
                logging.info(f"📹 Initial recording state: {state_name}")
                self.emit('recording_state_detected', is_recording)
                return

            logging.info("📹 RECORDING_STATE (0xD21D) not found in camera bulk data")

        except Exception as e:
            logging.warning(f"Recording state detection failed: {e}")
        finally:
            for evt, handler in [('operationData', on_data), ('operationResponse', on_response)]:
                handlers = self._event_handlers.get(evt, [])
                if handler in handlers:
                    handlers.remove(handler)

    async def ensure_sdio_ready(self):
        """Ensure SDIO ready before vendor control ops"""
        if self.sdio_ready:
            return True
        
        if self._sdio_ready_promise:
            # SDIO setup already in progress -- poll until ready
            for _ in range(40):  # up to 2s
                if self.sdio_ready:
                    return True
                time.sleep(0.05)
            return self.sdio_ready
        
        self._sdio_ready_promise = True
        self._establish_sdio_connection()
        # Poll until ready instead of blind sleep
        for _ in range(40):  # up to 2s
            if self.sdio_ready:
                return True
            time.sleep(0.05)
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
    
    # --- Recording Control ---
    
    async def start_recording(self) -> bool:
        """Start movie recording"""
        if not self.is_ready():
            logging.error("❌ Camera not ready for recording")
            return False
        
        await self.ensure_sdio_ready()
        logging.info("🎬 Starting recording...")
        try:
            return await self._send_recording_command(0x02)
        except Exception as e:
            logging.error(f"❌ Failed to start recording: {e}")
            return False
    
    async def stop_recording(self) -> bool:
        """Stop movie recording"""
        if not self.is_ready():
            logging.error("❌ Camera not ready to stop recording")
            return False
        
        await self.ensure_sdio_ready()
        logging.info("⏹️ Stopping recording...")
        try:
            return await self._send_recording_command(0x01)
        except Exception as e:
            logging.error(f"❌ Failed to stop recording: {e}")
            return False
    
    async def _send_recording_command(self, value: int) -> bool:
        """Send recording start/stop via SDIO_CONTROL_DEVICE with MOVIE_RECORDING property"""
        payload = struct.pack('<H', value)
        
        op_code = PTP_OPERATIONS['SDIO_CONTROL_DEVICE']
        prop_code = SONY_PROPERTIES['MOVIE_RECORDING']
        
        tx_id = self.transaction_id
        self.transaction_id += 1
        
        oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
            op_code, tx_id, [prop_code, 0], payload
        )
        
        try:
            self.connection.command_socket.send(oper_req)
            self.connection.command_socket.send(start_data)
            self.connection.command_socket.send(data_packet)
            self.connection.command_socket.send(end_data)
            logging.info(f"✅ Recording command sent (value=0x{value:02X})")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to send recording command: {e}")
            return False
    
    # --- Remote Touch Focus ---

    def remote_touch_sync(self, x: int, y: int) -> bool:
        """Send Remote Touch Operation via SDIO_ControlDevice (0xD2E4).
        Simulates a touch on the camera screen at the given coordinates.
        Action depends on camera's Touch Function setting (Focus, Tracking, Shutter, AE).
        Coordinates: X 0-639, Y 0-479. Packed as UINT32: upper 16 bits = X, lower 16 bits = Y.
        Fire-and-forget (Notch type).
        """
        if not self.is_ready() or not self.sdio_ready:
            return False

        # Clamp to valid range
        x = max(0, min(639, x))
        y = max(0, min(479, y))

        value = (x << 16) | y
        payload = struct.pack('<I', value)

        op_code = PTP_OPERATIONS['SDIO_CONTROL_DEVICE']
        prop_code = SONY_PROPERTIES['REMOTE_TOUCH']

        tx_id = self.transaction_id
        self.transaction_id += 1

        oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
            op_code, tx_id, [prop_code, 0], payload
        )

        try:
            self.connection.command_socket.send(oper_req)
            self.connection.command_socket.send(start_data)
            self.connection.command_socket.send(data_packet)
            self.connection.command_socket.send(end_data)
            logging.info(f"📍 Remote touch at ({x}, {y})")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to send remote touch: {e}")
            return False

    def cancel_remote_touch_sync(self) -> bool:
        """Cancel Remote Touch Operation via SDIO_ControlDevice (0xD2E5).
        Clears the current touch focus/tracking point.
        Button type (0x81): must send Down (0x0002) then Up (0x0001).
        The action executes on the Up event.
        """
        if not self.is_ready() or not self.sdio_ready:
            return False

        op_code = PTP_OPERATIONS['SDIO_CONTROL_DEVICE']
        prop_code = SONY_PROPERTIES['REMOTE_TOUCH_CANCEL']

        try:
            # Step 1: Send Down (0x0002) - press the button
            payload_down = struct.pack('<H', 0x0002)
            tx_id = self.transaction_id
            self.transaction_id += 1
            oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
                op_code, tx_id, [prop_code, 0], payload_down
            )
            self.connection.command_socket.send(oper_req)
            self.connection.command_socket.send(start_data)
            self.connection.command_socket.send(data_packet)
            self.connection.command_socket.send(end_data)

            # Step 2: Send Up (0x0001) - release triggers the action
            payload_up = struct.pack('<H', 0x0001)
            tx_id = self.transaction_id
            self.transaction_id += 1
            oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
                op_code, tx_id, [prop_code, 0], payload_up
            )
            self.connection.command_socket.send(oper_req)
            self.connection.command_socket.send(start_data)
            self.connection.command_socket.send(data_packet)
            self.connection.command_socket.send(end_data)

            logging.info("📍 Remote touch cancelled (focus point cleared)")
            return True
        except Exception as e:
            logging.error(f"❌ Failed to cancel remote touch: {e}")
            return False

    # --- Live View ---
    
    _liveview_initialized = False  # True after GetObjectInfo has been called once
    _last_focal_frames = []  # Latest parsed focal frame info (tracking/AF boxes)
    _focal_frame_logged = False  # One-time debug log flag
    
    # --- FocalFrameInfo Enums ---
    FOCUS_FRAME_TYPE = {
        0x0001: 'PhaseDetection_AFSensor', 0x0002: 'PhaseDetection_ImageSensor',
        0x0003: 'Wide', 0x0004: 'Zone', 0x0005: 'CentralEmphasis',
        0x0006: 'ContrastFlexibleMain', 0x0007: 'ContrastFlexibleAssist',
        0x0008: 'Contrast', 0x0009: 'ContrastUpperHalf', 0x000A: 'ContrastLowerHalf',
        0x000B: 'DualAFMain', 0x000C: 'DualAFAssist',
        0x000D: 'NonDualAFMain', 0x000E: 'NonDualAFAssist',
        0x000F: 'FrameSomewhere', 0x0010: 'Cross',
    }
    FOCUS_FRAME_STATE = {
        0x0001: 'NotFocused', 0x0002: 'Focused', 0x0003: 'FocusFrameSelection',
        0x0004: 'Moving', 0x0005: 'RangeLimit', 0x0006: 'RegistrationAF',
        0x0007: 'Island',
    }
    FACE_FRAME_TYPE = {
        0x0001: 'DetectedFace', 0x0002: 'AF_TargetFace',
        0x0003: 'PersonalRecognitionFace', 0x0004: 'SmileDetectionFace',
        0x0005: 'SelectedFace', 0x0006: 'AF_TargetSelectionFace',
        0x0007: 'SmileDetectionSelectFace',
    }
    FACE_FRAME_STATE = {0x0001: 'NotFocused', 0x0002: 'Focused'}
    TRACKING_FRAME_TYPE = {0x0001: 'NonTargetAF', 0x0002: 'TargetAF'}
    TRACKING_FRAME_STATE = {0x0001: 'NotFocused', 0x0002: 'Focused'}
    FRAMING_FRAME_TYPE = {
        0x0001: 'Auto', 0x0002: 'None', 0x0003: 'Single',
        0x0005: 'PTZ', 0x0008: 'HoldCurrentPosition', 0x0009: 'ForceZoomOut',
    }
    
    def _parse_focal_frame_info(self, ff_data: bytes):
        """Parse FocalFrameInfo binary block from LiveView Dataset.
        
        Structure (from Sony PTP 3 Reference):
          Information:  Version(UINT16) + reserved(6)             = 8 bytes
          reserved:     UINT8[40]                                 = 40 bytes
          reservedArrayNum(UINT16) + reserved(6)                  = 8 bytes
          reservedArray: N * 24 bytes each
          FocusFrame:   X_Den(4) + Y_Den(4) + FrameNum(2) + reserved(6)
                        + N * 24-byte Frame entries
          FaceFrame:    (version >= 1.01) same header, 24-byte entries
          TrackingFrame:(version >= 1.01) same header, 24-byte entries
          reserved(8) + reservedArrayNum(2) + reserved(6) + N * 16 bytes
          FramingFrame: (version >= 1.03) same header, 24-byte entries
        
        All coordinates are 1024x values: real = Numerator / Denominator.
        """
        frames = []
        try:
            if len(ff_data) < 56:  # minimum: 8 (info) + 40 (reserved) + 8 (array header)
                self._last_focal_frames = []
                return
            
            pos = 0
            
            # --- Information header (8 bytes) ---
            version = struct.unpack_from('<H', ff_data, pos)[0]  # 100x value (e.g. 100=1.00, 101=1.01)
            pos += 8  # 2 (version) + 6 (reserved)
            
            # --- reserved block (40 bytes) ---
            pos += 40
            
            # --- First reserved array ---
            if pos + 8 > len(ff_data):
                self._last_focal_frames = []
                return
            reserved_arr_num = struct.unpack_from('<H', ff_data, pos)[0]
            pos += 8  # 2 (count) + 6 (reserved)
            pos += reserved_arr_num * 24  # skip variable-length reserved entries
            
            # --- FocusFrame section ---
            frames.extend(self._parse_frame_section(
                ff_data, pos, 'focus', self.FOCUS_FRAME_TYPE, self.FOCUS_FRAME_STATE,
                frame_entry_parser=self._parse_focus_frame_entry
            ))
            pos = self._skip_frame_section(ff_data, pos)
            
            # --- FaceFrame section (version >= 1.01) ---
            if version >= 101 and pos < len(ff_data):
                frames.extend(self._parse_frame_section(
                    ff_data, pos, 'face', self.FACE_FRAME_TYPE, self.FACE_FRAME_STATE,
                    frame_entry_parser=self._parse_face_frame_entry
                ))
                pos = self._skip_frame_section(ff_data, pos)
            
            # --- TrackingFrame section (version >= 1.01) ---
            if version >= 101 and pos < len(ff_data):
                frames.extend(self._parse_frame_section(
                    ff_data, pos, 'tracking', self.TRACKING_FRAME_TYPE, self.TRACKING_FRAME_STATE,
                    frame_entry_parser=self._parse_tracking_frame_entry
                ))
                pos = self._skip_frame_section(ff_data, pos)
            
            # --- Second reserved block ---
            if pos + 16 <= len(ff_data):
                pos += 8  # 8 reserved bytes
                reserved_arr_num2 = struct.unpack_from('<H', ff_data, pos)[0]
                pos += 8  # 2 (count) + 6 (reserved)
                pos += reserved_arr_num2 * 16  # skip second reserved array
            
            # --- FramingFrame section (version >= 1.03) ---
            if version >= 103 and pos < len(ff_data):
                frames.extend(self._parse_frame_section(
                    ff_data, pos, 'framing', self.FRAMING_FRAME_TYPE, {},
                    frame_entry_parser=self._parse_framing_frame_entry
                ))
            
        except Exception as e:
            logging.debug(f"FocalFrameInfo parse error: {e}")
        
        self._last_focal_frames = frames
        
        # One-time debug log when we first see focal frame data
        if frames and not self._focal_frame_logged:
            self._focal_frame_logged = True
            logging.debug(f"FocalFrameInfo v{version/100:.2f}: {len(frames)} frame(s) — {frames}")
    
    def _skip_frame_section(self, data: bytes, pos: int) -> int:
        """Skip past a frame section (header + variable entries) and return new offset."""
        if pos + 16 > len(data):
            return len(data)
        # X_Den(4) + Y_Den(4) + FrameNum(2) + reserved(6) = 16 byte header
        frame_num = struct.unpack_from('<H', data, pos + 8)[0]
        return pos + 16 + frame_num * 24
    
    def _parse_frame_section(self, data, pos, category, type_map, state_map, frame_entry_parser):
        """Parse a frame section (Focus/Face/Tracking/Framing) and return frame dicts."""
        results = []
        if pos + 16 > len(data):
            return results
        
        x_den = struct.unpack_from('<I', data, pos)[0]
        y_den = struct.unpack_from('<I', data, pos + 4)[0]
        frame_num = struct.unpack_from('<H', data, pos + 8)[0]
        entry_offset = pos + 16  # skip header (4+4+2+6)
        
        for i in range(frame_num):
            if entry_offset + 24 > len(data):
                break
            entry = frame_entry_parser(data, entry_offset, x_den, y_den, type_map, state_map)
            if entry:
                entry['category'] = category
                results.append(entry)
            entry_offset += 24
        
        return results
    
    def _parse_focus_frame_entry(self, data, pos, x_den, y_den, type_map, state_map):
        """Parse a 24-byte FocusFrame entry:
        Type(2) + State(2) + Priority(1) + reserved(3) + X_Num(4) + Y_Num(4) + H(4) + W(4)"""
        ftype = struct.unpack_from('<H', data, pos)[0]
        fstate = struct.unpack_from('<H', data, pos + 2)[0]
        priority = data[pos + 4]
        x_num = struct.unpack_from('<I', data, pos + 8)[0]
        y_num = struct.unpack_from('<I', data, pos + 12)[0]
        height = struct.unpack_from('<I', data, pos + 16)[0]
        width = struct.unpack_from('<I', data, pos + 20)[0]
        return self._build_frame_dict(ftype, fstate, priority, x_num, y_num, width, height, x_den, y_den, type_map, state_map)
    
    def _parse_face_frame_entry(self, data, pos, x_den, y_den, type_map, state_map):
        """Parse a 24-byte FaceFrame entry:
        Type(2) + State(2) + Selection(1) + Priority(1) + reserved(2) + X_Num(4) + Y_Num(4) + H(4) + W(4)"""
        ftype = struct.unpack_from('<H', data, pos)[0]
        fstate = struct.unpack_from('<H', data, pos + 2)[0]
        selection = data[pos + 4]  # 0x01=Unselected, 0x02=Selected
        priority = data[pos + 5]
        x_num = struct.unpack_from('<I', data, pos + 8)[0]
        y_num = struct.unpack_from('<I', data, pos + 12)[0]
        height = struct.unpack_from('<I', data, pos + 16)[0]
        width = struct.unpack_from('<I', data, pos + 20)[0]
        d = self._build_frame_dict(ftype, fstate, priority, x_num, y_num, width, height, x_den, y_den, type_map, state_map)
        if d:
            d['selected'] = (selection == 0x02)
        return d
    
    def _parse_tracking_frame_entry(self, data, pos, x_den, y_den, type_map, state_map):
        """Parse a 24-byte TrackingFrame entry:
        Type(2) + State(2) + Priority(1) + reserved(3) + X_Num(4) + Y_Num(4) + H(4) + W(4)"""
        ftype = struct.unpack_from('<H', data, pos)[0]
        fstate = struct.unpack_from('<H', data, pos + 2)[0]
        priority = data[pos + 4]
        x_num = struct.unpack_from('<I', data, pos + 8)[0]
        y_num = struct.unpack_from('<I', data, pos + 12)[0]
        height = struct.unpack_from('<I', data, pos + 16)[0]
        width = struct.unpack_from('<I', data, pos + 20)[0]
        return self._build_frame_dict(ftype, fstate, priority, x_num, y_num, width, height, x_den, y_den, type_map, state_map)
    
    def _parse_framing_frame_entry(self, data, pos, x_den, y_den, type_map, state_map):
        """Parse a 24-byte FramingFrame entry:
        Type(2) + reserved(2) + Priority(1) + reserved(3) + X_Num(4) + Y_Num(4) + H(4) + W(4)"""
        ftype = struct.unpack_from('<H', data, pos)[0]
        priority = data[pos + 4]
        x_num = struct.unpack_from('<I', data, pos + 8)[0]
        y_num = struct.unpack_from('<I', data, pos + 12)[0]
        height = struct.unpack_from('<I', data, pos + 16)[0]
        width = struct.unpack_from('<I', data, pos + 20)[0]
        d = self._build_frame_dict(ftype, 0, priority, x_num, y_num, width, height, x_den, y_den, type_map, state_map)
        if d:
            d['state_name'] = ''  # Framing frames don't have a state
        return d
    
    @staticmethod
    def _build_frame_dict(ftype, fstate, priority, x_num, y_num, width, height,
                          x_den, y_den, type_map, state_map):
        """Build a normalized frame dict from raw values."""
        if x_den == 0 or y_den == 0:
            return None
        # Normalized coordinates (0-1 range): center and dimensions
        cx = x_num / x_den
        cy = y_num / y_den
        w = width / x_den
        h = height / y_den
        # Skip zero-size frames (empty placeholders)
        if w == 0 and h == 0:
            return None
        return {
            'type': ftype,
            'type_name': type_map.get(ftype, f'0x{ftype:04X}'),
            'state': fstate,
            'state_name': state_map.get(fstate, f'0x{fstate:04X}'),
            'priority': priority,
            'cx': round(cx, 4),
            'cy': round(cy, 4),
            'w': round(w, 4),
            'h': round(h, 4),
        }
    
    def get_focal_frames(self) -> list:
        """Get the latest parsed focal frame info (tracking/AF/face boxes)."""
        return self._last_focal_frames
    
    def _init_liveview(self) -> bool:
        """Call GetObjectInfo(0xFFFFC002) once before streaming, as recommended by the PTP spec.
        Also verifies that Live View Status is enabled (camera in shooting mode).
        Returns True if the camera is ready for liveview.
        """
        if self._liveview_initialized:
            return True
        
        if not self.is_ready() or not self.sdio_ready:
            return False
        
        logging.debug("Initializing live view (GetObjectInfo)...")
        
        try:
            # Send GetObjectInfo(0xFFFFC002) - call once for performance
            tx_id = self.transaction_id
            self.transaction_id += 1
            
            result_holder = [None]
            error_holder = [None]
            done_event = threading.Event()
            
            def on_data(resp_tx_id, payload):
                if resp_tx_id == tx_id:
                    result_holder[0] = payload
                    done_event.set()
            
            def on_response(resp_tx_id, response_code):
                if resp_tx_id == tx_id:
                    if response_code != PTP_RESPONSES['OK']:
                        error_holder[0] = response_code
                    done_event.set()
            
            self.on('operationData', on_data)
            self.on('operationResponse', on_response)
            
            packet = create_operation_request(
                PTP_OPERATIONS['GET_OBJECT_INFO'],
                tx_id,
                [LIVEVIEW_HANDLE]
            )
            self.connection.command_socket.send(packet)
            
            done_event.wait(timeout=3.0)
            
            # Clean up listeners
            for evt, handler in [('operationData', on_data), ('operationResponse', on_response)]:
                handlers = self._event_handlers.get(evt, [])
                if handler in handlers:
                    handlers.remove(handler)
            
            if error_holder[0]:
                logging.debug(f"GetObjectInfo(liveview) failed: 0x{error_holder[0]:04X}")
                return False
            
            if result_holder[0] is not None:
                logging.debug(f"Live view initialized (ObjectInfo: {len(result_holder[0])} bytes)")
                self._liveview_initialized = True
                return True
            
            # Timeout - could mean camera doesn't support liveview
            logging.debug("GetObjectInfo(liveview) timed out")
            return False
            
        except Exception as e:
            logging.warning(f"Failed to initialize liveview: {e}")
            return False
    
    def disable_liveview(self):
        """Reset liveview state (called when stream ends)."""
        self._liveview_initialized = False
    
    def get_liveview_frame_sync(self) -> Optional[bytes]:
        """Get a single liveview JPEG frame via GetObject(0xFFFFC002).
        
        Synchronous method designed to be called from Flask threads.
        Uses the background data handler thread + event system to collect the response.
        
        The camera returns a LiveView Dataset with this structure:
          Bytes 0-3:   Offset to Live View Image (UINT32)
          Bytes 4-7:   Live View Image Size (UINT32)  
          Bytes 8-11:  Offset to Focal Frame Info (UINT32)
          Bytes 12-15: Focal Frame Info Size (UINT32)
          Then reserved bytes, then JPEG data at the specified offset.
        
        Returns raw JPEG bytes or None on failure/timeout.
        """
        if not self.is_ready() or not self.sdio_ready:
            return None
        
        if not self.connection.command_socket:
            return None
        
        # Initialize liveview on first frame request (GetObjectInfo once)
        if not self._liveview_initialized:
            if not self._init_liveview():
                return None
        
        tx_id = self.transaction_id
        self.transaction_id += 1
        
        # One-shot listeners for either data response or error response
        result_holder = [None]
        error_holder = [None]
        done_event = threading.Event()
        
        def on_data(resp_tx_id, payload):
            if resp_tx_id == tx_id:
                result_holder[0] = payload
                done_event.set()
        
        def on_response(resp_tx_id, response_code):
            if resp_tx_id == tx_id:
                if response_code != PTP_RESPONSES['OK']:
                    error_holder[0] = response_code
                if result_holder[0] is None:
                    # Error response without data - unblock
                    done_event.set()
        
        self.on('operationData', on_data)
        self.on('operationResponse', on_response)
        
        try:
            # Send GetObject request with liveview handle
            packet = create_operation_request(
                PTP_OPERATIONS['GET_OBJECT'],
                tx_id,
                [LIVEVIEW_HANDLE]
            )
            self.connection.command_socket.send(packet)
            
            # Wait for the background thread to assemble and deliver the data
            done_event.wait(timeout=2.0)
            
            # Handle errors
            if error_holder[0]:
                err = error_holder[0]
                if err == 0x200F:
                    # Access_Denied - too fast, retry next cycle (normal)
                    pass
                elif err == 0x2009:
                    # Invalid ObjectHandle - liveview not enabled
                    logging.debug("Liveview not available (0x2009 - camera may not be in shooting mode)")
                    self._liveview_initialized = False
                else:
                    logging.debug(f"Liveview GetObject error: 0x{err:04X}")
                return None
            
            data = result_holder[0]
            if not data or len(data) < 16:
                return None
            
            # Parse LiveView Dataset header
            img_offset = struct.unpack('<I', data[0:4])[0]
            img_size = struct.unpack('<I', data[4:8])[0]
            
            if img_size == 0:
                # Image size zero = no frame ready yet, retry
                return None
            
            # Parse FocalFrameInfo if present (bytes 8-15)
            if len(data) >= 16:
                ff_offset = struct.unpack('<I', data[8:12])[0]
                ff_size = struct.unpack('<I', data[12:16])[0]
                if ff_size > 0 and ff_offset + ff_size <= len(data):
                    self._parse_focal_frame_info(data[ff_offset:ff_offset + ff_size])
                else:
                    self._last_focal_frames = []
            
            # Extract JPEG from the dataset
            if img_offset + img_size <= len(data):
                jpeg_data = data[img_offset:img_offset + img_size]
                # Validate JPEG SOI marker
                if len(jpeg_data) > 2 and jpeg_data[0:2] == b'\xff\xd8':
                    return jpeg_data
                else:
                    logging.debug(f"LiveView data at offset {img_offset} is not JPEG "
                                  f"(first 4: {jpeg_data[:4].hex() if len(jpeg_data) >= 4 else 'short'})")
            else:
                # Fallback: search for JPEG SOI marker in the raw data
                soi = data.find(b'\xff\xd8')
                if soi >= 0:
                    return data[soi:]
                logging.debug(f"LiveView Dataset: offset={img_offset} size={img_size} "
                              f"but data only {len(data)} bytes")
            
            return None
            
        except Exception as e:
            logging.debug(f"Failed to get liveview frame: {e}")
            return None
        finally:
            # Remove the one-shot listeners
            for evt, handler in [('operationData', on_data), ('operationResponse', on_response)]:
                handlers = self._event_handlers.get(evt, [])
                if handler in handlers:
                    handlers.remove(handler)
    
    # --- Device Property Get/Set ---

    # Enum lookup tables keyed by property code for formatting values
    _ENUM_TABLES = {
        SONY_PROPERTIES['WHITE_BALANCE']: WHITE_BALANCE_VALUES,
        SONY_PROPERTIES['FOCUS_MODE']: FOCUS_MODE_VALUES,
        SONY_PROPERTIES['EXPOSURE_PROGRAM_MODE']: EXPOSURE_PROGRAM_VALUES,
        SONY_PROPERTIES['SHUTTER_MODE']: SHUTTER_MODE_VALUES,
        SONY_PROPERTIES['TOUCH_FUNCTION']: TOUCH_FUNCTION_VALUES,
        SONY_PROPERTIES['ZOOM_SETTING']: ZOOM_SETTING_VALUES,
        SONY_PROPERTIES['ZOOM_TYPE_STATUS']: ZOOM_TYPE_STATUS_VALUES,
    }

    def get_device_property_sync(self, prop_code: int, timeout: float = 3.0) -> dict:
        """Read a single device property by fetching all via SDIO_GetAllExtDevicePropInfo (0x9209).
        Returns a dict with raw, display, allowed, enabled, settable -- or None on failure.
        """
        # Reverse-lookup: prop_code -> property name used in get_all_properties_sync result
        prop_name_map = {
            SONY_PROPERTIES['WHITE_BALANCE']: 'white_balance',
            SONY_PROPERTIES['F_NUMBER']: 'f_number',
            SONY_PROPERTIES['FOCUS_MODE']: 'focus_mode',
            SONY_PROPERTIES['ISO']: 'iso',
            SONY_PROPERTIES['SHUTTER_SPEED']: 'shutter_speed',
            SONY_PROPERTIES['EXPOSURE_PROGRAM_MODE']: 'exposure_program',
            SONY_PROPERTIES['SHUTTER_MODE']: 'shutter_mode',
            SONY_PROPERTIES['SHUTTER_ANGLE']: 'shutter_angle',
            SONY_PROPERTIES['TOUCH_FUNCTION']: 'touch_function',
            SONY_PROPERTIES['REMOTE_TOUCH_ENABLE']: 'remote_touch_enable',
            SONY_PROPERTIES['REMOTE_TOUCH_CANCEL_ENABLE']: 'remote_touch_cancel_enable',
            SONY_PROPERTIES['ZOOM_SETTING']: 'zoom_setting',
            SONY_PROPERTIES['ZOOM_TYPE_STATUS']: 'zoom_type_status',
        }
        name = prop_name_map.get(prop_code)
        if not name:
            logging.debug(f"get_device_property_sync: unknown prop 0x{prop_code:04X}")
            return None

        all_props = self.get_all_properties_sync(timeout=timeout)
        prop = all_props.get(name)
        if prop and (prop.get('raw') != 0 or prop.get('display') != '--'):
            return prop
        return None

    def set_device_property_sync(self, prop_code: int, value: int, timeout: float = 2.0) -> bool:
        """Set a device property via SDIO_CONTROL_DEVICE (0x9207).
        Uses the same fire-and-forget data-phase approach as recording/zoom.
        The property code is passed as a parameter and the value as the data payload.
        """
        if not self.is_ready() or not self.sdio_ready:
            return False

        tx_id = self.transaction_id
        self.transaction_id += 1

        # Pack value based on size: UINT32 props need 4 bytes, others use 2
        uint32_props = {
            SONY_PROPERTIES.get('ISO'),
            SONY_PROPERTIES.get('SHUTTER_SPEED'),
            SONY_PROPERTIES.get('EXPOSURE_PROGRAM_MODE'),
            SONY_PROPERTIES.get('SHUTTER_ANGLE'),
        }
        if prop_code in uint32_props:
            payload = struct.pack('<I', value)
        else:
            payload = struct.pack('<H', value)

        try:
            oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
                PTP_OPERATIONS['SDIO_CONTROL_DEVICE'],
                tx_id,
                [prop_code, 0],
                payload
            )

            self.connection.command_socket.send(oper_req)
            self.connection.command_socket.send(start_data)
            self.connection.command_socket.send(data_packet)
            self.connection.command_socket.send(end_data)

            logging.info(f"Property 0x{prop_code:04X} set to 0x{value:04X}")
            return True
        except Exception as e:
            logging.error(f"set_device_property_sync(0x{prop_code:04X}) failed: {e}")
            return False

    def get_all_properties_sync(self, timeout: float = 3.0) -> dict:
        """Read all camera properties in one shot via SDIO_GET_ALL_EXT_DEVICE_PROP_INFO (0x9209).
        Parses WB, F-Number, Focus Mode, ISO, Shutter Speed, Exposure Program from the bulk response.
        Returns a dict keyed by property name.
        """
        empty = lambda: {'raw': 0, 'display': '--', 'allowed': [], 'enabled': False, 'settable': False}
        default_result = {
            'white_balance': empty(), 'f_number': empty(), 'focus_mode': empty(),
            'iso': empty(), 'shutter_speed': empty(), 'exposure_program': empty(),
            'shutter_mode': empty(), 'shutter_angle': empty(),
            'touch_function': empty(), 'remote_touch_enable': empty(),
            'remote_touch_cancel_enable': empty(),
            'zoom_setting': empty(), 'zoom_type_status': empty(),
            'media_slot1_rec_time': empty(), 'media_slot2_rec_time': empty(),
            'battery_remaining': empty(),
            'metered_manual_level': empty(),
        }

        if not self.is_ready() or not self.sdio_ready:
            return default_result

        # Fetch all device property data via single 0x9209 call
        tx_id = self.transaction_id
        self.transaction_id += 1

        result_holder = [None]
        error_holder = [None]
        done_event = threading.Event()

        def on_data(resp_tx_id, payload):
            if resp_tx_id == tx_id:
                result_holder[0] = payload
                done_event.set()

        def on_response(resp_tx_id, response_code):
            if resp_tx_id == tx_id:
                if response_code != PTP_RESPONSES['OK']:
                    error_holder[0] = response_code
                if result_holder[0] is None:
                    done_event.set()

        self.on('operationData', on_data)
        self.on('operationResponse', on_response)

        try:
            packet = create_operation_request(
                PTP_OPERATIONS['SDIO_GET_ALL_EXT_DEVICE_PROP_INFO'],
                tx_id, []
            )
            self.connection.command_socket.send(packet)
            done_event.wait(timeout=timeout)

            if error_holder[0]:
                logging.warning(f"GetAllDevicePropData error: 0x{error_holder[0]:04X}")
                return default_result

            data = result_holder[0]
            if not data or len(data) < 20:
                logging.debug("GetAllDevicePropData returned no data")
                return default_result

            logging.debug(f"GetAllDevicePropData: {len(data)} bytes")

            # Property codes we want to extract
            want_codes = {
                SONY_PROPERTIES['WHITE_BALANCE']: 'white_balance',
                SONY_PROPERTIES['F_NUMBER']: 'f_number',
                SONY_PROPERTIES['FOCUS_MODE']: 'focus_mode',
                SONY_PROPERTIES['ISO']: 'iso',
                SONY_PROPERTIES['SHUTTER_SPEED']: 'shutter_speed',
                SONY_PROPERTIES['EXPOSURE_PROGRAM_MODE']: 'exposure_program',
                SONY_PROPERTIES['SHUTTER_MODE']: 'shutter_mode',
                SONY_PROPERTIES['SHUTTER_ANGLE']: 'shutter_angle',
                SONY_PROPERTIES['TOUCH_FUNCTION']: 'touch_function',
                SONY_PROPERTIES['REMOTE_TOUCH_ENABLE']: 'remote_touch_enable',
                SONY_PROPERTIES['REMOTE_TOUCH_CANCEL_ENABLE']: 'remote_touch_cancel_enable',
                SONY_PROPERTIES['ZOOM_SETTING']: 'zoom_setting',
                SONY_PROPERTIES['ZOOM_TYPE_STATUS']: 'zoom_type_status',
                SONY_PROPERTIES['MEDIA_SLOT1_REC_TIME']: 'media_slot1_rec_time',
                SONY_PROPERTIES['MEDIA_SLOT2_REC_TIME']: 'media_slot2_rec_time',
                SONY_PROPERTIES['BATTERY_REMAINING']: 'battery_remaining',
                SONY_PROPERTIES['METERED_MANUAL_LEVEL']: 'metered_manual_level',
            }

            props = dict(default_result)

            # Targeted byte-search for each property code in the bulk data.
            # The bulk data has an 8-byte header and the sequential parser is
            # fragile (loses alignment on unknown descriptors).  Searching for
            # each 2-byte property code and then parsing the descriptor at that
            # offset is robust regardless of header/alignment.
            #
            # Descriptor layout (Camera Control PTP 3 Reference):
            #   PropertyCode(2) + DataType(2) + GetSet(1) + IsEnabled(1)
            #   + Reserved(N) + CurrentValue(N) + FormFlag(1) [ + form data ]
            # where N = value size of the DataType.

            dt_size = {0x0001: 1, 0x0002: 1, 0x0003: 2, 0x0004: 2,
                       0x0005: 4, 0x0006: 4, 0x0007: 8, 0x0008: 8}

            for prop_code, name in want_codes.items():
                needle = struct.pack('<H', prop_code)
                idx = data.find(needle)
                if idx == -1 or idx + 6 > len(data):
                    continue

                datatype = struct.unpack('<H', data[idx + 2:idx + 4])[0]
                val_size = dt_size.get(datatype, 0)
                if val_size == 0:
                    continue

                get_set = data[idx + 4]
                is_enabled = data[idx + 5]

                # CurrentValue sits after Reserved(val_size)
                cv_offset = idx + 6 + val_size
                if cv_offset + val_size > len(data):
                    continue

                if val_size == 1:
                    current_value = data[cv_offset]
                elif val_size == 2:
                    current_value = struct.unpack('<H', data[cv_offset:cv_offset + 2])[0]
                elif val_size == 4:
                    current_value = struct.unpack('<I', data[cv_offset:cv_offset + 4])[0]
                elif val_size == 8:
                    current_value = struct.unpack('<Q', data[cv_offset:cv_offset + 8])[0]
                else:
                    current_value = 0

                # FormFlag + form data (for allowed values)
                form_offset = cv_offset + val_size
                form_flag = data[form_offset] if form_offset < len(data) else 0
                allowed = []
                if form_flag == 0x02:  # Enumeration
                    enum_offset = form_offset + 1
                    if enum_offset + 2 <= len(data):
                        num_vals = struct.unpack('<H', data[enum_offset:enum_offset + 2])[0]
                        enum_offset += 2
                        if num_vals <= 200:
                            for _ in range(num_vals):
                                if enum_offset + val_size <= len(data):
                                    if val_size == 1:
                                        allowed.append(data[enum_offset])
                                    elif val_size == 2:
                                        allowed.append(struct.unpack('<H', data[enum_offset:enum_offset + 2])[0])
                                    elif val_size == 4:
                                        allowed.append(struct.unpack('<I', data[enum_offset:enum_offset + 4])[0])
                                    enum_offset += val_size
                                else:
                                    break

                # For Sony UINT32 packed properties, extract the meaningful 16-bit value
                # (used for enum lookups on properties that pack a mode+value in 32 bits)
                display_value = current_value
                if datatype == 0x0006 and current_value > 0xFFFF:
                    lo16 = current_value & 0xFFFF
                    hi16 = (current_value >> 16) & 0xFFFF
                    if prop_code == SONY_PROPERTIES['EXPOSURE_PROGRAM_MODE']:
                        display_value = lo16 if lo16 != 0 else hi16

                # Format display string
                if prop_code == SONY_PROPERTIES['F_NUMBER']:
                    display = format_fnumber(current_value)
                elif prop_code == SONY_PROPERTIES['SHUTTER_SPEED']:
                    display = format_shutter_speed(current_value)
                elif prop_code == SONY_PROPERTIES['SHUTTER_ANGLE']:
                    display = format_shutter_angle(current_value)
                elif prop_code == SONY_PROPERTIES['ISO']:
                    display = format_iso(current_value)
                elif prop_code == SONY_PROPERTIES['BATTERY_REMAINING']:
                    # INT8: 0xFF = untaken/no battery, otherwise percentage
                    if current_value == 0xFF or current_value == 0:
                        display = '--'
                    else:
                        display = f'{current_value}%'
                elif prop_code == SONY_PROPERTIES['METERED_MANUAL_LEVEL']:
                    # INT16 signed: value * 1000 = EV
                    signed = current_value if current_value < 0x8000 else current_value - 0x10000
                    ev = signed / 1000.0
                    display_value = ev
                    if ev > 0:
                        display = f'+{ev:.1f}'
                    elif ev < 0:
                        display = f'{ev:.1f}'
                    else:
                        display = '0.0'
                elif prop_code in (SONY_PROPERTIES['MEDIA_SLOT1_REC_TIME'],
                                   SONY_PROPERTIES['MEDIA_SLOT2_REC_TIME']):
                    secs = current_value
                    if secs == 0 or secs == 0xFFFFFFFF:
                        display = '--'
                    elif secs >= 3600:
                        h = secs // 3600
                        m = (secs % 3600) // 60
                        display = f'{h}h{m}m'
                    elif secs >= 60:
                        m = secs // 60
                        display = f'{m}m'
                    else:
                        display = f'{secs}s'
                elif prop_code in self._ENUM_TABLES:
                    display = self._ENUM_TABLES[prop_code].get(
                        display_value,
                        self._ENUM_TABLES[prop_code].get(current_value, f'0x{display_value:04X}')
                    )
                else:
                    display = str(display_value) if display_value != current_value else str(current_value)

                props[name] = {
                    'raw': display_value,
                    'display': display,
                    'allowed': allowed,
                    'enabled': bool(is_enabled),
                    'settable': get_set == 0x01,
                }

                logging.debug(f"  Found 0x{prop_code:04X} ({name}): val={current_value} "
                              f"display={display} get_set={get_set} enabled={is_enabled}")

            return props

        except Exception as e:
            logging.error(f"get_all_properties_sync failed: {e}")
            return default_result
        finally:
            for evt, handler in [('operationData', on_data), ('operationResponse', on_response)]:
                handlers = self._event_handlers.get(evt, [])
                if handler in handlers:
                    handlers.remove(handler)

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
                logging.debug("Camera not ready for zoom bar query")
                return None
            
            logging.debug("Querying zoom bar info")
            
            # Use SDIO_GetAllExtDevicePropInfo to get all device properties
            op_code = PTP_OPERATIONS['SDIO_GET_ALL_EXT_DEVICE_PROP_INFO']  # 0x9209
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
                            logging.debug(f"Found zoom property at offset {offset} but no valid data")
                
                except Exception as e:
                    logging.error(f"Error parsing zoom bar data: {e}")
            
            return None
            
        except Exception as e:
            logging.error(f"Failed to get zoom bar info: {e}")
            return None