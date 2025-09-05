"""
Sony Camera PTP-IP Client - Main Client Class
"""

import time
import threading
import logging
import random
from threading import Timer
from typing import Optional

from .protocol import (
    PTP_STATES, PTPIP_INIT_COMMAND_ACK, PTPIP_INIT_EVENT_ACK, PTPIP_OPER_RESP,
    PTPIP_PROBE_REQ, PTPIP_PROBE_RESP, PTPIP_START_DATA, PTPIP_DATA, PTPIP_END_DATA,
    PTPIP_EVENT, PTP_OPERATIONS, PTP_RESPONSES,
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
        
        logging.info('🔗 Connecting to Sony Alpha camera...')
        
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
                    send_operation_sync(PTP_OPERATIONS['SONY_GET_ALL_DEVICE_PROP_DATA'], [])
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
    
    async def disconnect(self):
        """Disconnect from camera"""
        self._stop_task_loop()
        self._stop_keepalive()
        
        # Stop zoom tracking
        if hasattr(self.zoom, '_zoom_tracking_interval') and self.zoom._zoom_tracking_interval:
            self.zoom._zoom_tracking_interval.cancel()
        
        await self.connection.disconnect()
        
        self.connected = False
        self.state = PTP_STATES.INIT
        logging.info('Disconnected from camera')
