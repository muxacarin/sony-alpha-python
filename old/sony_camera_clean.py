#!/usr/bin/env python3
"""
Sony Camera PTP-IP Client with Task Loop
Clean production version for pedal mapper integration
"""

import socket
import struct
import time
import threading
import logging
import subprocess
import random
from typing import Optional, List
from threading import Timer

# PTP-IP Packet Types
PTPIP_INIT_COMMAND_REQUEST = 0x00000001
PTPIP_INIT_COMMAND_ACK = 0x00000002
PTPIP_INIT_EVENT_REQUEST = 0x00000003
PTPIP_INIT_EVENT_ACK = 0x00000004
PTPIP_INIT_FAIL = 0x00000005
PTPIP_OPER_REQ = 0x00000006
PTPIP_OPER_RESP = 0x00000007
PTPIP_EVENT = 0x00000008
PTPIP_START_DATA = 0x00000009
PTPIP_DATA = 0x0000000A
PTPIP_CANCEL = 0x0000000B
PTPIP_END_DATA = 0x0000000C
PTPIP_PROBE_REQ = 0x0000000D
PTPIP_PROBE_RESP = 0x0000000E
PTPIP_PORT = 15740

# PTP States
class PTP_STATES:
    INIT = 0
    START_WAIT = 1
    SOCK_CONN = 2
    CMD_REQ = 3
    EVENT_REQ = 4
    OPEN_SESSION = 5
    READY = 6

class SonyCamera:
    """Sony Camera PTP-IP client with task loop (production version)"""
    
    def __init__(self, ip_address: str, ssh_username: str = None, ssh_password: str = None):
        self.ip_address = ip_address
        self.port = PTPIP_PORT
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        
        # Connection state
        self.command_socket: Optional[socket.socket] = None
        self.event_socket: Optional[socket.socket] = None
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
        
        # SSH tunnel
        self.ssh_process: Optional[subprocess.Popen] = None
        self.ssh_tunnel_active = False
        
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
        
        # Zoom tracking
        self._virtual_zoom = {'percent': 0.5, 'min': 0, 'max': 100, 'step': 1}
        self._zoom_tracking_interval = None
    
    def _generate_guid(self) -> bytes:
        """Generate a 16-byte GUID for camera pairing"""
        return bytes([random.randint(0, 255) for _ in range(16)])
    
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
    
    async def connect(self) -> bool:
        """Connect to camera"""
        if self.connected:
            return True
        
        logging.info('🔗 Connecting to Sony Alpha camera...')
        
        try:
            # Establish SSH tunnel if needed
            if self.ssh_username and self.ssh_password:
                if not await self._establish_ssh_tunnel():
                    return False
                target_host = 'localhost'
            else:
                target_host = self.ip_address
            
            # Connect both channels
            await self._connect_command_channel(target_host)
            return True
            
        except Exception as e:
            logging.error(f'❌ Connection failed: {e}')
            await self.disconnect()
            return False
    
    async def _establish_ssh_tunnel(self) -> bool:
        """Establish SSH tunnel"""
        try:
            cmd = [
                'sshpass', '-p', self.ssh_password,
                'ssh', '-c', 'aes128-ctr', '-N', '-L', '15740:localhost:15740',
                '-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null',
                '-o', 'PreferredAuthentications=keyboard-interactive',
                '-o', 'KbdInteractiveAuthentication=yes', '-o', 'PubkeyAuthentication=no',
                '-o', 'ConnectTimeout=10', f'{self.ssh_username}@{self.ip_address}'
            ]
            
            self.ssh_process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            time.sleep(4)  # Give SSH time to establish
            
            if self.ssh_process.poll() is None:
                logging.info("✅ SSH tunnel established")
                self.ssh_tunnel_active = True
                return True
            else:
                logging.error("SSH tunnel failed")
                return False
                
        except Exception as e:
            logging.error(f"SSH tunnel error: {e}")
            return False
    
    async def _connect_command_channel(self, target_host: str):
        """Connect both channels simultaneously"""
        self.command_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.event_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        self.state = PTP_STATES.START_WAIT
        
        try:
            # Set socket options
            self.command_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            self.event_socket.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            
            # Connect both channels
            self.command_socket.connect((target_host, self.port))
            self.event_socket.connect((target_host, self.port))
            
            logging.info('📡 Both channels connected')
            self.state = PTP_STATES.SOCK_CONN
            
            # Set up data handlers
            self._setup_command_data_handler()
            self._setup_event_data_handler()
            
            # Grace period before starting handshake
            Timer(0.15, self._start_handshake).start()
            
        except Exception as e:
            raise Exception(f"Channel connection failed: {e}")
    
    def _setup_command_data_handler(self):
        """Set up command channel data handling"""
        def handle_data():
            try:
                while True:
                    data = self.command_socket.recv(4096)
                    if not data:
                        break
                    
                    self._cmd_buffer += data
                    while len(self._cmd_buffer) >= 8:
                        pkt_len = struct.unpack('<I', self._cmd_buffer[:4])[0]
                        if pkt_len == 0 or pkt_len > 1_000_000:
                            self._cmd_buffer = b''
                            break
                        if len(self._cmd_buffer) < pkt_len:
                            break
                        
                        pkt = self._cmd_buffer[:pkt_len]
                        self._cmd_buffer = self._cmd_buffer[pkt_len:]
                        self._handle_command_data(pkt)
                        
            except Exception as e:
                logging.debug(f"Command data handler error: {e}")
        
        threading.Thread(target=handle_data, daemon=True).start()
    
    def _setup_event_data_handler(self):
        """Set up event channel data handling"""
        def handle_data():
            try:
                while True:
                    data = self.event_socket.recv(4096)
                    if not data:
                        break
                    
                    self._event_buffer += data
                    while len(self._event_buffer) >= 8:
                        pkt_len = struct.unpack('<I', self._event_buffer[:4])[0]
                        if pkt_len == 0 or pkt_len > 1_000_000:
                            self._event_buffer = b''
                            break
                        if len(self._event_buffer) < pkt_len:
                            break
                        
                        pkt = self._event_buffer[:pkt_len]
                        self._event_buffer = self._event_buffer[pkt_len:]
                        self._handle_event_data(pkt)
                        
            except Exception as e:
                logging.debug(f"Event data handler error: {e}")
        
        threading.Thread(target=handle_data, daemon=True).start()
    
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
                cmd_req_packet = self._create_init_cmd_req_packet()
                self.command_socket.send(cmd_req_packet)
                logging.info('📤 Sent INIT_CMD_REQ')
                self.state = PTP_STATES.CMD_REQ + 1
                
            elif self.state == PTP_STATES.EVENT_REQ:
                if not self.connection_id:
                    return
                
                event_req_packet = self._create_init_event_req_packet()
                self.event_socket.send(event_req_packet)
                logging.info('📤 Sent INIT_EVENT_REQ')
                self.state = PTP_STATES.EVENT_REQ + 1
                
            elif self.state == PTP_STATES.OPEN_SESSION:
                self.session_id = random.randint(1, 0xFFFFFF)
                session_packet = self._create_operation_request(0x1002, [self.session_id])
                self.command_socket.send(session_packet)
                logging.info(f'📤 OpenSession (ID: {self.session_id})')
                self.state = PTP_STATES.OPEN_SESSION + 1
                
        except Exception as e:
            logging.error(f"Task loop error: {e}")
            self.emit('handshakeFailed', e)
    
    def _handle_command_data(self, data: bytes):
        """Handle command channel data"""
        if len(data) < 8:
            return
        
        self.last_activity = time.time()
        
        packet_type = struct.unpack('<I', data[4:8])[0]
        
        if packet_type == PTPIP_INIT_COMMAND_ACK:
            self._handle_init_cmd_ack(data)
        elif packet_type == PTPIP_OPER_RESP:
            self._handle_operation_response(data)
        elif packet_type == PTPIP_PROBE_REQ:
            self._send_probe_resp(self.command_socket)
        elif packet_type == PTPIP_PROBE_RESP:
            logging.debug('💓 Received keepalive PROBE_RESP')
        elif packet_type == PTPIP_START_DATA:
            tx_id = struct.unpack('<I', data[8:12])[0]
            pending_len = struct.unpack('<I', data[12:16])[0]
            self._inbound_data = {'tx_id': tx_id, 'pending_len': pending_len, 'chunks': []}
        elif packet_type == PTPIP_DATA:
            tx_id = struct.unpack('<I', data[8:12])[0]
            payload = data[12:]
            if self._inbound_data and self._inbound_data['tx_id'] == tx_id:
                self._inbound_data['chunks'].append(payload)
        elif packet_type == PTPIP_END_DATA:
            tx_id = struct.unpack('<I', data[8:12])[0]
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
        
        packet_type = struct.unpack('<I', data[4:8])[0]
        
        if packet_type == PTPIP_INIT_EVENT_ACK:
            self._handle_init_event_ack(data)
        elif packet_type == PTPIP_PROBE_REQ:
            self._send_probe_resp(self.event_socket)
        elif packet_type == PTPIP_PROBE_RESP:
            logging.debug('💓 Received keepalive PROBE_RESP on event channel')
        elif packet_type == PTPIP_EVENT:
            if len(data) >= 10:
                event_code = struct.unpack('<H', data[8:10])[0]
                self.emit('cameraEvent', event_code, data)
    
    def _handle_init_cmd_ack(self, data: bytes):
        """Handle INIT_CMD_ACK"""
        logging.info('✅ Pairing accepted!')
        
        if len(data) >= 12:
            self.connection_id = struct.unpack('<I', data[8:12])[0]
            
            if len(data) >= 28:
                if len(data) > 28:
                    name_data = data[28:]
                    try:
                        for i in range(0, len(name_data) - 1, 2):
                            if name_data[i] == 0 and name_data[i + 1] == 0:
                                self.camera_name = name_data[:i].decode('utf-16le')
                                break
                        logging.info(f'📷 Camera: {self.camera_name}')
                    except:
                        pass
        
        self.state = PTP_STATES.EVENT_REQ
    
    def _handle_init_event_ack(self, data: bytes):
        """Handle INIT_EVENT_ACK"""
        logging.info('✅ Event channel ready')
        self.state = PTP_STATES.OPEN_SESSION
    
    def _handle_operation_response(self, data: bytes):
        """Handle operation response"""
        response_code = None
        if len(data) >= 10:
            response_code = struct.unpack('<H', data[8:10])[0]
        
        if self.state in [PTP_STATES.OPEN_SESSION, PTP_STATES.OPEN_SESSION + 1]:
            if response_code == 0x2001:  # OK
                if self.state == PTP_STATES.OPEN_SESSION:  # Only log once
                    logging.info('✅ Session opened successfully!')
                self.connected = True
                self.state = PTP_STATES.READY
                self._stop_task_loop()
                
                # Start SDIO establishment
                if not self.sdio_ready and not self._sdio_ready_promise:
                    self._establish_sdio_connection()
                
                self.emit('ready')
            elif response_code == 0x2013:  # Session Already Open - not actually an error during SDIO
                logging.debug('Session already open (0x2013) - continuing...')
            else:
                logging.error(f'❌ Session failed: 0x{response_code:04x}')
                self.emit('handshakeFailed', Exception(f'Session failed: 0x{response_code:04x}'))
    
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
                    packet = self._create_operation_request(op_code, params)
                    self.command_socket.send(packet)
                    time.sleep(0.1)
                    return True
                
                # SDIO sequence
                send_operation_sync(0x1001, [])  # GET_DEVICE_INFO
                time.sleep(0.05)
                send_operation_sync(0x1004, [])  # GET_STORAGE_IDS
                time.sleep(0.05)
                send_operation_sync(0x9201, [1, 0, 0])  # SDIO_CONNECT phase 1
                time.sleep(0.05)
                send_operation_sync(0x9201, [2, 0, 0])  # SDIO_CONNECT phase 2
                time.sleep(0.05)
                send_operation_sync(0x9202, [0x012C, 0, 0])  # SDIO_GET_EXT_DEVICE_INFO
                time.sleep(0.05)
                send_operation_sync(0x9201, [3, 0, 0])  # SDIO_CONNECT phase 3
                time.sleep(0.08)
                send_operation_sync(0x9202, [0x012C, 0, 0])  # Final GET_EXT_DEVICE_INFO
                
                logging.info('✅ SDIO sequence complete')
                self.sdio_ready = True
                
                try:
                    send_operation_sync(0x9209, [])  # SONY_GET_ALL_DEVICE_PROP_DATA
                    logging.info('✅ Camera fully ready!')
                except Exception as e:
                    logging.warning(f'Device properties failed: {e}')
                
            except Exception as e:
                logging.error(f'SDIO sequence failed: {e}')
                self.sdio_ready = False
        
        threading.Thread(target=run_sdio_sequence, daemon=True).start()
    
    def _create_init_cmd_req_packet(self) -> bytes:
        """Create INIT_CMD_REQ packet"""
        client_name = "PythonPTPClient"
        client_name_utf16 = client_name.encode('utf-16le') + b'\x00\x00'
        protocol_version = struct.pack('<I', 0x00010000)
        
        total_length = 4 + 4 + 16 + len(client_name_utf16) + 4
        packet = bytearray(total_length)
        offset = 0
        
        struct.pack_into('<I', packet, offset, total_length)
        offset += 4
        struct.pack_into('<I', packet, offset, PTPIP_INIT_COMMAND_REQUEST)
        offset += 4
        packet[offset:offset+16] = self.guid
        offset += 16
        packet[offset:offset+len(client_name_utf16)] = client_name_utf16
        offset += len(client_name_utf16)
        packet[offset:offset+4] = protocol_version
        
        return bytes(packet)
    
    def _create_init_event_req_packet(self) -> bytes:
        """Create INIT_EVENT_REQ packet"""
        return struct.pack('<III', 12, PTPIP_INIT_EVENT_REQUEST, self.connection_id)
    
    def _create_operation_request(self, op_code: int, params: List[int] = None) -> bytes:
        """Create PTP operation request"""
        if params is None:
            params = []
        
        base_length = 4 + 4 + 4 + 2 + 4
        total_length = base_length + (len(params) * 4)
        
        packet = bytearray(total_length)
        offset = 0
        
        struct.pack_into('<I', packet, offset, total_length)
        offset += 4
        struct.pack_into('<I', packet, offset, PTPIP_OPER_REQ)
        offset += 4
        struct.pack_into('<I', packet, offset, 0)  # Data Phase
        offset += 4
        struct.pack_into('<H', packet, offset, op_code)
        offset += 2
        struct.pack_into('<I', packet, offset, self.transaction_id)
        offset += 4
        self.transaction_id += 1
        
        for param in params:
            struct.pack_into('<I', packet, offset, param)
            offset += 4
        
        return bytes(packet)
    
    def _send_probe_resp(self, sock: socket.socket):
        """Send PROBE_RESP"""
        try:
            pkt = struct.pack('<II', 8, PTPIP_PROBE_RESP)
            sock.send(pkt)
        except Exception as e:
            logging.debug(f'Failed sending PROBE_RESP: {e}')
    
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
            pkt = struct.pack('<II', 8, PTPIP_PROBE_REQ)
            if self.command_socket:
                self.command_socket.send(pkt)
            if self.event_socket:
                self.event_socket.send(pkt)
        except Exception as e:
            logging.debug(f'Failed sending PROBE_REQ: {e}')
    
    async def disconnect(self):
        """Disconnect from camera"""
        self._stop_task_loop()
        self._stop_keepalive()
        
        if self.command_socket:
            try:
                self.command_socket.close()
            except:
                pass
            self.command_socket = None
        
        if self.event_socket:
            try:
                self.event_socket.close()
            except:
                pass
            self.event_socket = None
        
        if self.ssh_tunnel_active and self.ssh_process:
            try:
                self.ssh_process.terminate()
                self.ssh_process.wait(timeout=5)
            except:
                pass
            self.ssh_process = None
            self.ssh_tunnel_active = False
        
        self.connected = False
        self.state = PTP_STATES.INIT
        logging.info('Disconnected from camera')
    
    # Zoom Control Methods
    
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
    
    def is_ready(self):
        """Check if camera is ready"""
        return self.connected and self.state == PTP_STATES.READY
    
    async def start_zoom(self, direction: str, speed: int = 1):
        """Start continuous zoom with speed"""
        if not self.is_ready():
            raise Exception('Camera not ready')
        
        await self.ensure_sdio_ready()
        
        # Match Node.js logic exactly
        if direction == 'in':
            signed = max(1, speed)
        else:
            signed = -max(1, speed)
        
        signed = max(-127, min(127, signed))
        
        logging.info(f'🔍 Starting zoom {direction} (speed: {signed})')
        
        # Start virtual position tracking
        if self._zoom_tracking_interval:
            self._zoom_tracking_interval.cancel()
        
        def track_zoom():
            try:
                increment = 0.01 if direction == 'in' else -0.01
                new_percent = max(0, min(1, self._virtual_zoom['percent'] + increment))
                self._virtual_zoom['percent'] = new_percent
                
                self.emit('zoomPositionUpdate', {
                    'value': round(self._virtual_zoom['min'] + new_percent * (self._virtual_zoom['max'] - self._virtual_zoom['min'])),
                    'min': self._virtual_zoom['min'],
                    'max': self._virtual_zoom['max'],
                    'step': self._virtual_zoom['step'],
                    'percent': new_percent
                })
            except Exception as e:
                logging.debug(f'Zoom tracking error: {e}')
        
        def start_tracking():
            if self._zoom_tracking_interval:
                return
            track_zoom()
            self._zoom_tracking_interval = Timer(0.15, start_tracking)
            self._zoom_tracking_interval.start()
        
        start_tracking()
        
        return await self._send_zoom_command(signed)
    
    async def stop_zoom(self):
        """Stop continuous zoom"""
        if not self.is_ready():
            raise Exception('Camera not ready')
        
        await self.ensure_sdio_ready()
        
        # Stop virtual position tracking
        if self._zoom_tracking_interval:
            self._zoom_tracking_interval.cancel()
            self._zoom_tracking_interval = None
        
        logging.info('🔍 Stopping zoom')
        
        return await self._send_zoom_command(0)
    
    async def _send_zoom_command(self, signed_speed: int):
        """Send zoom command with data phase"""
        payload = struct.pack('b', signed_speed)
        return await self._send_operation_with_data(0x9207, [0xD2DD, 0], payload)
    
    async def _send_operation_with_data(self, op_code: int, params: List[int], payload: bytes):
        """Send operation with data phase"""
        if not self.command_socket:
            raise Exception('Command socket not available')
        
        # Create OPER_REQ with data phase = 1
        base_length = 4 + 4 + 4 + 2 + 4
        total_length = base_length + (len(params) * 4)
        
        packet = bytearray(total_length)
        offset = 0
        
        struct.pack_into('<I', packet, offset, total_length)
        offset += 4
        struct.pack_into('<I', packet, offset, PTPIP_OPER_REQ)
        offset += 4
        struct.pack_into('<I', packet, offset, 1)  # Data Phase
        offset += 4
        struct.pack_into('<H', packet, offset, op_code)
        offset += 2
        
        tx_id = self.transaction_id
        struct.pack_into('<I', packet, offset, tx_id)
        offset += 4
        self.transaction_id += 1
        
        for param in params:
            struct.pack_into('<I', packet, offset, param)
            offset += 4
        
        # START_DATA, DATA, END_DATA packets
        start_data = struct.pack('<IIIQ', 20, PTPIP_START_DATA, tx_id, len(payload))
        data_packet = struct.pack('<III', 12 + len(payload), PTPIP_DATA, tx_id) + payload
        end_data = struct.pack('<III', 12, PTPIP_END_DATA, tx_id)
        
        try:
            self.command_socket.send(bytes(packet))
            self.command_socket.send(start_data)
            self.command_socket.send(data_packet)
            self.command_socket.send(end_data)
            
            return 0x2001  # OK
            
        except Exception as e:
            logging.error(f'Failed to send zoom command: {e}')
            raise


# Simple test function for standalone usage
async def main():
    """Simple test runner"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    import sys
    if len(sys.argv) >= 4:
        ip = sys.argv[1]
        username = sys.argv[2]
        password = sys.argv[3]
    else:
        logging.error("Usage: python sony_camera_clean.py <ip> <username> <password>")
        return
    
    camera = SonyCamera(ip, username, password)
    
    try:
        success = await camera.connect()
        if success:
            logging.info("✅ Camera connected successfully!")
            
            # Wait for camera to be ready
            max_wait = 15
            waited = 0
            while not camera.is_ready() and waited < max_wait:
                time.sleep(0.5)
                waited += 0.5
            
            if camera.is_ready():
                logging.info("✅ Camera ready! Testing zoom...")
                
                # Quick zoom test
                await camera.start_zoom('in', 2)
                time.sleep(2)
                await camera.stop_zoom()
                
                time.sleep(1)
                
                await camera.start_zoom('out', 2)
                time.sleep(2)
                await camera.stop_zoom()
                
                logging.info("✅ Zoom test completed!")
            else:
                logging.error("❌ Camera didn't become ready")
                
        else:
            logging.error("❌ Connection failed")
            
    except KeyboardInterrupt:
        logging.info("Interrupted by user")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        await camera.disconnect()


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
