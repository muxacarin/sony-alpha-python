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
    "GET_DEVICE_INFO": 0x1001,
    "OPEN_SESSION": 0x1002,
    "GET_STORAGE_IDS": 0x1004,
    "SDIO_CONNECT": 0x9201,  # Authentication handshake
    "SDIO_GET_EXT_DEVICE_INFO": 0x9202,  # Get protocol version + supported properties
    "SDIO_SET_EXT_DEVICE_PROP": 0x9205,  # Set a DevicePropValue for a device property
    "SDIO_CONTROL_DEVICE": 0x9207,  # Set SDIControl value for SDIControlCode
    "SDIO_GET_ALL_EXT_DEVICE_PROP_INFO": 0x9209,  # Get all DevicePropDescs at once
    "SDIO_GET_EXT_DEVICE_PROP": 0x9251,  # Get DevicePropInfo (not supported on cinema cameras)
    "GET_OBJECT": 0x1009,
    "GET_OBJECT_INFO": 0x1008,
}

# Special Object Handles
LIVEVIEW_HANDLE = 0xFFFFC002  # Live view frame
STILL_IMAGE_HANDLE = 0xFFFFC001  # Captured still image

# Sony Property Codes
SONY_PROPERTIES = {
    "ZOOM_STEP": 0xD2DD,
    "ZOOM_SETTING": 0xD25F,  # Zoom Setting - UINT8, Get/Set (0x01=Optical, 0x02=Smart, 0x03=ClearImage, 0x04=Digital)
    "ZOOM_TYPE_STATUS": 0xD260,  # Zoom Type Status - UINT8, Get only
    "ZOOM_BAR_INFO": 0xD25D,
    "MOVIE_RECORDING": 0xD2C8,  # Movie recording start/stop
    "RECORDING_STATE": 0xD21D,  # Movie recording state (0=stop, 1=recording)
    "LIVEVIEW_STATUS": 0xD221,  # Live View Status (0x00=disabled, 0x01=enabled)
    # Standard PTP device properties
    "WHITE_BALANCE": 0x5005,
    "F_NUMBER": 0x5007,
    "FOCUS_MODE": 0x500A,
    "EXPOSURE_METERING_MODE": 0x500B,
    "EXPOSURE_PROGRAM_MODE": 0x500E,
    "EXPOSURE_BIAS": 0x5010,
    "ISO": 0xD21E,  # ISO Sensitivity - UINT32, Get/Set (lower 24 bits = value, upper byte = mode)
    "SHUTTER_SPEED": 0xD20D,  # Shutter Speed (FX30/FX3) - UINT32, Get/Set
    "SHUTTER_SPEED_FX6": 0xD017,  # Shutter Speed (FX6/higher-end) - Get only
    "SHUTTER_MODE": 0xD010,  # Shutter Mode - UINT8: 0x01=Speed, 0x02=Angle
    "SHUTTER_ANGLE": 0xD00E,  # Shutter Angle - UINT32, 1000x real value
    "AF_AREA_POSITION": 0xD2DC,  # AF Area Position - UINT32, upper 16=X(0-639), lower 16=Y(0-479), Notch type
    # Remote Touch Operation
    "REMOTE_TOUCH": 0xD2E4,  # Remote Touch Operation (x,y) - UINT32, Notch type via SDIO_ControlDevice
    "REMOTE_TOUCH_CANCEL": 0xD2E5,  # Cancel Remote Touch Operation - UINT16, Button type (0x0001=Up, 0x0002=Down)
    "REMOTE_TOUCH_ENABLE": 0xD284,  # Remote Touch Operation Enable Status - UINT8, Get only (0x00=Disable, 0x01=Enable)
    "REMOTE_TOUCH_CANCEL_ENABLE": 0xD285,  # Cancel Remote Touch Operation Enable Status - UINT8, Get only
    "TOUCH_FUNCTION": 0xD283,  # Function of Touch Operation - UINT8, Get/Set
    # Storage / recording time remaining
    "MEDIA_SLOT1_REC_TIME": 0xD24A,  # Media SLOT1 remaining recording time (seconds) - UINT32
    "MEDIA_SLOT2_REC_TIME": 0xD258,  # Media SLOT2 remaining recording time (seconds) - UINT32
    # Battery
    "BATTERY_REMAINING": 0xD218,  # Battery remaining % - INT8 (0x64=100%, 0xFF=untaken)
    # Exposure metering
    "METERED_MANUAL_LEVEL": 0xD1B5,  # Metered manual level - INT16 (value * 1000)
}

# --- Value Enums ---

WHITE_BALANCE_VALUES = {
    0x0001: "Manual",
    0x0002: "AWB",
    0x0003: "One-push Auto",
    0x0004: "Daylight",
    0x0005: "Fluorescent",
    0x0006: "Tungsten",
    0x0007: "Flash",
    0x8001: "Fluor: Warm White",
    0x8002: "Fluor: Cool White",
    0x8003: "Fluor: Day White",
    0x8004: "Fluor: Daylight",
    0x8010: "Cloudy",
    0x8011: "Shade",
    0x8012: "C.Temp",
    0x8020: "Custom 1",
    0x8021: "Custom 2",
    0x8022: "Custom 3",
    0x8023: "Custom",
    0x8030: "Underwater Auto",
}

FOCUS_MODE_VALUES = {
    0x0001: "MF",
    0x0002: "AF-S",
    0x0003: "AF Macro",
    0x8004: "AF-C",
    0x8005: "AF-A",
    0x8006: "DMF",
}

TOUCH_FUNCTION_VALUES = {
    0x01: "OFF",
    0x02: "Touch Shutter",
    0x03: "Touch Focus",
    0x04: "Touch Tracking",
    0x05: "Touch AE",
    0x06: "Touch Shutter + AE",
    0x07: "Touch Shutter",
    0x08: "Touch Focus + AE",
    0x09: "Touch Focus",
    0x0A: "Touch Tracking + AE",
    0x0B: "Touch Tracking",
}

SHUTTER_MODE_VALUES = {
    0x01: "Speed",
    0x02: "Angle",
}

EXPOSURE_PROGRAM_VALUES = {
    0x0001: "M",
    0x0002: "P",
    0x0003: "A",
    0x0004: "S",
    0x0007: "Scene",
    0x8010: "Intelligent Auto",
    0x8011: "Superior Auto",
    0x8050: "Movie P",
    0x8051: "Movie A",
    0x8052: "Movie S",
    0x8053: "Movie M",
    0x8080: "S&Q P",
    0x8081: "S&Q A",
    0x8082: "S&Q S",
    0x8083: "S&Q M",
    0x8090: "Movie",
    0x8091: "Cine EI",
    0x8092: "Cine EI Quick",
}

ZOOM_SETTING_VALUES = {
    0x01: "Optical Only",
    0x02: "Smart Zoom",
    0x03: "Clear Image Zoom",
    0x04: "Digital Zoom",
}

ZOOM_TYPE_STATUS_VALUES = {
    0x01: "Optical",
    0x02: "Smart",
    0x03: "Clear Image",
    0x04: "Digital",
}


def format_fnumber(raw: int) -> str:
    """Format raw F-Number value (100x) to display string like 'f/4.5'"""
    if raw == 0xFFFD:
        return "Closed"
    if raw in (0xFFFE, 0xFFFF, 0):
        return "--"
    return f"f/{raw / 100:.1f}"


def format_shutter_speed(raw: int) -> str:
    """Format raw shutter speed value to display string.
    Upper 16 bits = numerator, Lower 16 bits = denominator.
    - 0x00000000 = BULB
    - 0xFFFFFFFF = nothing to display
    - Denominator 0x000A (10) = "real number" display, e.g. 1.5"
    - Numerator 0x0001 = fraction display, e.g. 1/1000
    """
    if raw == 0x00000000:
        return "BULB"
    if raw == 0xFFFFFFFF:
        return "--"
    numerator = (raw >> 16) & 0xFFFF
    denominator = raw & 0xFFFF
    if denominator == 0:
        return "--"
    if denominator == 10:
        # "Real number" display: numerator / 10  (e.g. 15/10 = 1.5")
        val = numerator / 10
        if val == int(val):
            return f'{int(val)}"'
        return f'{val:.1f}"'
    if numerator == 1:
        # Fraction display: 1/denominator
        return f"1/{denominator}"
    # Generic fraction
    return f"{numerator}/{denominator}"


def format_iso(raw: int) -> str:
    """Format raw ISO value (UINT32, property 0xD21E).
    Lower 24 bits = ISO value, upper 8 bits = mode flag.
    0x00 = normal, 0x01 = Multi Frame NR, 0x02 = Multi Frame NR High.
    0x00FFFFFF / 0x01FFFFFF / 0x02FFFFFF = Auto variants.
    Extended ISO: offset 0x10000000.
    """
    if raw == 0:
        return "Auto"

    mode = (raw >> 24) & 0xFF
    iso_val = raw & 0x00FFFFFF

    # Handle extended ISO flag
    extended = bool(mode & 0x10)
    mode_base = mode & 0x0F

    if iso_val == 0x00FFFFFF:
        # Auto
        if mode_base == 0x01:
            return "MF NR Auto"
        elif mode_base == 0x02:
            return "MF NR Hi Auto"
        return "Auto"

    # Format the ISO number
    prefix = ""
    if mode_base == 0x01:
        prefix = "MF NR "
    elif mode_base == 0x02:
        prefix = "MF NR Hi "

    return f"{prefix}ISO {iso_val}"


def format_shutter_angle(raw: int) -> str:
    """Format raw shutter angle value to display string.
    Value is 1000x the real angle. 0x00000000 = nothing to display.
    e.g. 180000 = 180°, 90000 = 90°
    """
    if raw == 0x00000000:
        return "--"
    angle = raw / 1000
    if angle == int(angle):
        return f"{int(angle)}°"
    return f"{angle:.1f}°"


# PTP Response Codes
PTP_RESPONSES = {
    "OK": 0x2001,
    "SESSION_ALREADY_OPEN": 0x2013,
    "DEVICE_BUSY": 0x2019,
}


def create_init_cmd_req_packet(guid: bytes) -> bytes:
    """Create INIT_CMD_REQ packet"""
    client_name = "PythonPTPClient"
    client_name_utf16 = client_name.encode("utf-16le") + b"\x00\x00"
    protocol_version = struct.pack("<I", 0x00010000)

    total_length = 4 + 4 + 16 + len(client_name_utf16) + 4
    packet = bytearray(total_length)
    offset = 0

    struct.pack_into("<I", packet, offset, total_length)
    offset += 4
    struct.pack_into("<I", packet, offset, PTPIP_INIT_COMMAND_REQUEST)
    offset += 4
    packet[offset : offset + 16] = guid
    offset += 16
    packet[offset : offset + len(client_name_utf16)] = client_name_utf16
    offset += len(client_name_utf16)
    packet[offset : offset + 4] = protocol_version

    return bytes(packet)


def create_init_event_req_packet(connection_id: int) -> bytes:
    """Create INIT_EVENT_REQ packet"""
    return struct.pack("<III", 12, PTPIP_INIT_EVENT_REQUEST, connection_id)


def create_operation_request(
    op_code: int, transaction_id: int, params: List[int] = None
) -> bytes:
    """Create PTP operation request"""
    if params is None:
        params = []

    base_length = 4 + 4 + 4 + 2 + 4
    total_length = base_length + (len(params) * 4)

    packet = bytearray(total_length)
    offset = 0

    struct.pack_into("<I", packet, offset, total_length)
    offset += 4
    struct.pack_into("<I", packet, offset, PTPIP_OPER_REQ)
    offset += 4
    struct.pack_into("<I", packet, offset, 0)  # Data Phase
    offset += 4
    struct.pack_into("<H", packet, offset, op_code)
    offset += 2
    struct.pack_into("<I", packet, offset, transaction_id)
    offset += 4

    for param in params:
        struct.pack_into("<I", packet, offset, param)
        offset += 4

    return bytes(packet)


def create_operation_with_data_packets(
    op_code: int, transaction_id: int, params: List[int], payload: bytes
):
    """Create operation with data phase packets (OPER_REQ, START_DATA, DATA, END_DATA)"""
    if params is None:
        params = []

    # OPER_REQ with data phase = 1
    base_length = 4 + 4 + 4 + 2 + 4
    total_length = base_length + (len(params) * 4)

    packet = bytearray(total_length)
    offset = 0

    struct.pack_into("<I", packet, offset, total_length)
    offset += 4
    struct.pack_into("<I", packet, offset, PTPIP_OPER_REQ)
    offset += 4
    struct.pack_into("<I", packet, offset, 0x2)  # Data Phase
    offset += 4
    struct.pack_into("<H", packet, offset, op_code)
    offset += 2
    struct.pack_into("<I", packet, offset, transaction_id)
    offset += 4

    for param in params:
        struct.pack_into("<I", packet, offset, param)
        offset += 4

    # START_DATA, DATA, END_DATA packets
    start_data = struct.pack(
        "<IIIQ", 20, PTPIP_START_DATA, transaction_id, len(payload)
    )
    data_packet = (
        struct.pack("<III", 12 + len(payload), PTPIP_DATA, transaction_id) + payload
    )
    end_data = struct.pack("<III", 12, PTPIP_END_DATA, transaction_id)

    return bytes(packet), start_data, data_packet, end_data


def create_probe_resp() -> bytes:
    """Create PROBE_RESP packet"""
    return struct.pack("<II", 8, PTPIP_PROBE_RESP)


def create_probe_req() -> bytes:
    """Create PROBE_REQ packet"""
    return struct.pack("<II", 8, PTPIP_PROBE_REQ)


def parse_packet_header(data: bytes) -> tuple:
    """Parse PTP-IP packet header, returns (length, packet_type)"""
    if len(data) < 8:
        return None, None

    length = struct.unpack("<I", data[:4])[0]
    packet_type = struct.unpack("<I", data[4:8])[0]
    return length, packet_type


def parse_init_cmd_ack(data: bytes) -> dict:
    """Parse INIT_CMD_ACK packet"""
    result = {}

    if len(data) >= 12:
        result["connection_id"] = struct.unpack("<I", data[8:12])[0]

        if len(data) >= 28:
            result["camera_guid"] = data[12:28]

            if len(data) > 28:
                # Extract camera name (UTF-16)
                name_data = data[28:]
                try:
                    for i in range(0, len(name_data) - 1, 2):
                        if name_data[i] == 0 and name_data[i + 1] == 0:
                            result["camera_name"] = name_data[:i].decode("utf-16le")
                            break
                except:
                    pass

    return result


def parse_operation_response(data: bytes) -> dict:
    """Parse operation response packet"""
    result = {}

    if len(data) >= 10:
        result["response_code"] = struct.unpack("<H", data[8:10])[0]

        if len(data) >= 14:
            result["transaction_id"] = struct.unpack("<I", data[10:14])[0]

    return result
