"""
Sony Camera Zoom Control
"""

import struct
import time
import logging
from threading import Timer
from .protocol import (
    SONY_PROPERTIES, 
    PTP_OPERATIONS,
    create_operation_with_data_packets
)

class ZoomController:
    """Handles zoom control functionality"""
    
    def __init__(self, camera_client):
        self.camera = camera_client
        
        # Virtual zoom tracking (like Node.js)
        self._virtual_zoom = {'percent': 0.5, 'min': 0, 'max': 100, 'step': 1}
        self._zoom_tracking_interval = None
    
    async def start_zoom(self, direction: str, speed: int = 1):
        """Start continuous zoom with speed"""
        if not self.camera.is_ready():
            logging.error("❌ Camera not ready for zoom")
            raise Exception('Camera not ready')
        
        logging.info("🔧 Ensuring SDIO ready...")
        await self.camera.ensure_sdio_ready()
        logging.info("✅ SDIO ready confirmed")
        
        # Match Node.js logic exactly
        if direction == 'in':
            signed = max(1, speed)
        else:
            signed = -max(1, speed)
        
        signed = max(-127, min(127, signed))
        
        logging.info(f'🔍 Starting zoom {direction} (speed: {signed}) - VERSION 2.0 WITH DEBUG!')
        
        # Start virtual position tracking
        try:
            logging.info("🔧 Setting up virtual zoom tracking...")
            if self._zoom_tracking_interval:
                self._zoom_tracking_interval.cancel()
            
            def track_zoom():
                try:
                    increment = 0.01 if direction == 'in' else -0.01
                    new_percent = max(0, min(1, self._virtual_zoom['percent'] + increment))
                    self._virtual_zoom['percent'] = new_percent
                    
                    self.camera.emit('zoomPositionUpdate', {
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
            logging.info("✅ Virtual zoom tracking started")
        except Exception as e:
            logging.error(f"❌ Failed to start virtual tracking (continuing anyway): {e}")
            # Continue with zoom command even if tracking fails
        
        return await self._send_zoom_command(signed)
    
    async def stop_zoom(self):
        """Stop continuous zoom"""
        if not self.camera.is_ready():
            logging.error("❌ Camera not ready for zoom stop")
            raise Exception('Camera not ready')
        
        logging.info("🔧 Ensuring SDIO ready for zoom stop...")
        await self.camera.ensure_sdio_ready()
        logging.info("✅ SDIO ready confirmed for zoom stop")
        
        # Stop virtual position tracking
        if self._zoom_tracking_interval:
            self._zoom_tracking_interval.cancel()
            self._zoom_tracking_interval = None
        
        logging.info('🔍 Stopping zoom')
        
        return await self._send_zoom_command(0)
    
    async def _send_zoom_command(self, signed_speed: int):
        """Send zoom command with data phase"""
        logging.info(f"🔧 Sending zoom command: speed={signed_speed}")
        payload = struct.pack('b', signed_speed)
        
        try:
            result = await self._send_operation_with_data(
                PTP_OPERATIONS['SONY_SET_CONTROL_DEVICE_B'], 
                [SONY_PROPERTIES['ZOOM_STEP'], 0], 
                payload
            )
            logging.info(f"✅ Zoom command sent successfully: result={result}")
            return result
        except Exception as e:
            logging.error(f"❌ Failed to send zoom command: {e}")
            raise
    
    async def _send_operation_with_data(self, op_code: int, params: list, payload: bytes):
        """Send operation with data phase"""
        logging.info(f"🔧 Preparing zoom operation: op_code=0x{op_code:04X}, params={params}, payload_len={len(payload)}")
        
        if not self.camera.connection.command_socket:
            raise Exception('Command socket not available')
        
        # Get current transaction ID and increment
        tx_id = self.camera.transaction_id
        self.camera.transaction_id += 1
        logging.info(f"🔧 Using transaction ID: {tx_id}")
        
        # Create all packets
        oper_req, start_data, data_packet, end_data = create_operation_with_data_packets(
            op_code, tx_id, params, payload
        )
        
        try:
            # Send all packets in sequence
            logging.info(f"🔧 Sending operation request packet ({len(oper_req)} bytes)")
            self.camera.connection.command_socket.send(oper_req)
            
            logging.info(f"🔧 Sending start data packet ({len(start_data)} bytes)")
            self.camera.connection.command_socket.send(start_data)
            
            logging.info(f"🔧 Sending data packet ({len(data_packet)} bytes)")
            self.camera.connection.command_socket.send(data_packet)
            
            logging.info(f"🔧 Sending end data packet ({len(end_data)} bytes)")
            self.camera.connection.command_socket.send(end_data)
            
            logging.info("✅ All zoom packets sent successfully")
            return 0x2001  # OK
            
        except Exception as e:
            logging.error(f'❌ Failed to send zoom command packets: {e}')
            raise
