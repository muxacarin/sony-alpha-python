"""
Sony Camera PTP-IP Protocol Constants and Packet Handling
"""

import struct
from typing import List

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

# PTP Operation Codes
PTP_OPERATIONS = {
    'GET_DEVICE_INFO': 0x1001,
    'OPEN_SESSION': 0x1002,
    'GET_STORAGE_IDS': 0x1004,
    'SDIO_CONNECT': 0x9201,
    'SDIO_GET_EXT_DEVICE_INFO': 0x9202,
    'SONY_SET_CONTROL_DEVICE_B': 0x9207,
    'SONY_GET_ALL_DEVICE_PROP_DATA': 0x9209,
}

# Sony Property Codes
SONY_PROPERTIES = {
    'ZOOM_STEP': 0xD2DD,
}

# PTP Response Codes
PTP_RESPONSES = {
    'OK': 0x2001,
    'SESSION_ALREADY_OPEN': 0x2013,
    'DEVICE_BUSY': 0x2019,
}

def create_init_cmd_req_packet(guid: bytes) -> bytes:
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
    packet[offset:offset+16] = guid
    offset += 16
    packet[offset:offset+len(client_name_utf16)] = client_name_utf16
    offset += len(client_name_utf16)
    packet[offset:offset+4] = protocol_version
    
    return bytes(packet)

def create_init_event_req_packet(connection_id: int) -> bytes:
    """Create INIT_EVENT_REQ packet"""
    return struct.pack('<III', 12, PTPIP_INIT_EVENT_REQUEST, connection_id)

def create_operation_request(op_code: int, transaction_id: int, params: List[int] = None) -> bytes:
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
    struct.pack_into('<I', packet, offset, transaction_id)
    offset += 4
    
    for param in params:
        struct.pack_into('<I', packet, offset, param)
        offset += 4
    
    return bytes(packet)

def create_operation_with_data_packets(op_code: int, transaction_id: int, params: List[int], payload: bytes):
    """Create operation with data phase packets (OPER_REQ, START_DATA, DATA, END_DATA)"""
    if params is None:
        params = []
    
    # OPER_REQ with data phase = 1
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
    struct.pack_into('<I', packet, offset, transaction_id)
    offset += 4
    
    for param in params:
        struct.pack_into('<I', packet, offset, param)
        offset += 4
    
    # START_DATA, DATA, END_DATA packets
    start_data = struct.pack('<IIIQ', 20, PTPIP_START_DATA, transaction_id, len(payload))
    data_packet = struct.pack('<III', 12 + len(payload), PTPIP_DATA, transaction_id) + payload
    end_data = struct.pack('<III', 12, PTPIP_END_DATA, transaction_id)
    
    return bytes(packet), start_data, data_packet, end_data

def create_probe_resp() -> bytes:
    """Create PROBE_RESP packet"""
    return struct.pack('<II', 8, PTPIP_PROBE_RESP)

def create_probe_req() -> bytes:
    """Create PROBE_REQ packet"""
    return struct.pack('<II', 8, PTPIP_PROBE_REQ)

def parse_packet_header(data: bytes) -> tuple:
    """Parse PTP-IP packet header, returns (length, packet_type)"""
    if len(data) < 8:
        return None, None
    
    length = struct.unpack('<I', data[:4])[0]
    packet_type = struct.unpack('<I', data[4:8])[0]
    return length, packet_type

def parse_init_cmd_ack(data: bytes) -> dict:
    """Parse INIT_CMD_ACK packet"""
    result = {}
    
    if len(data) >= 12:
        result['connection_id'] = struct.unpack('<I', data[8:12])[0]
        
        if len(data) >= 28:
            result['camera_guid'] = data[12:28]
            
            if len(data) > 28:
                # Extract camera name (UTF-16)
                name_data = data[28:]
                try:
                    for i in range(0, len(name_data) - 1, 2):
                        if name_data[i] == 0 and name_data[i + 1] == 0:
                            result['camera_name'] = name_data[:i].decode('utf-16le')
                            break
                except:
                    pass
    
    return result

def parse_operation_response(data: bytes) -> dict:
    """Parse operation response packet"""
    result = {}
    
    if len(data) >= 10:
        result['response_code'] = struct.unpack('<H', data[8:10])[0]
        
        if len(data) >= 14:
            result['transaction_id'] = struct.unpack('<I', data[10:14])[0]
    
    return result
