"""
Sony Camera Zoom Control
"""

import logging
import struct
import time
from threading import Timer

from .protocol import (
    PTP_OPERATIONS,
    SONY_PROPERTIES,
    create_operation_with_data_packets,
)


class ZoomController:
    """Handles zoom control functionality"""

    def __init__(self, camera_client):
        self.camera = camera_client

        # Virtual zoom tracking (like Node.js)
        self._virtual_zoom = {"percent": 0.5, "min": 0, "max": 100, "step": 1}
        self._zoom_tracking_interval = None

    async def start_zoom(self, direction: str, speed: int = 1):
        """Start continuous zoom with speed"""
        if not self.camera.is_ready():
            raise Exception("Camera not ready")

        await self.camera.ensure_sdio_ready()

        # Match Node.js logic exactly
        if direction == "in":
            signed = max(1, speed)
        else:
            signed = -max(1, speed)

        signed = max(-127, min(127, signed))

        logging.debug(f"Zoom {direction} speed={signed}")

        # Start virtual position tracking
        try:
            if self._zoom_tracking_interval:
                self._zoom_tracking_interval.cancel()

            def track_zoom():
                try:
                    increment = 0.01 if direction == "in" else -0.01
                    new_percent = max(
                        0, min(1, self._virtual_zoom["percent"] + increment)
                    )
                    self._virtual_zoom["percent"] = new_percent

                    self.camera.emit(
                        "zoomPositionUpdate",
                        {
                            "value": round(
                                self._virtual_zoom["min"]
                                + new_percent
                                * (
                                    self._virtual_zoom["max"]
                                    - self._virtual_zoom["min"]
                                )
                            ),
                            "min": self._virtual_zoom["min"],
                            "max": self._virtual_zoom["max"],
                            "step": self._virtual_zoom["step"],
                            "percent": new_percent,
                        },
                    )
                except Exception as e:
                    logging.debug(f"Zoom tracking error: {e}")

            def start_tracking():
                if self._zoom_tracking_interval:
                    return
                track_zoom()
                self._zoom_tracking_interval = Timer(0.15, start_tracking)
                self._zoom_tracking_interval.start()

            start_tracking()
        except Exception as e:
            logging.debug(f"Virtual zoom tracking failed (continuing): {e}")

        return await self._send_zoom_command(signed)

    async def stop_zoom(self):
        """Stop continuous zoom"""
        if not self.camera.is_ready():
            raise Exception("Camera not ready")

        await self.camera.ensure_sdio_ready()

        # Stop virtual position tracking
        if self._zoom_tracking_interval:
            self._zoom_tracking_interval.cancel()
            self._zoom_tracking_interval = None

        logging.debug("Zoom stop")

        return await self._send_zoom_command(0)

    async def _send_zoom_command(self, signed_speed: int):
        """Send zoom command with data phase"""
        payload = struct.pack("b", signed_speed)

        try:
            result = await self._send_operation_with_data(
                PTP_OPERATIONS["SDIO_CONTROL_DEVICE"],
                [SONY_PROPERTIES["ZOOM_STEP"], 0],
                payload,
            )
            return result
        except Exception as e:
            logging.error(f"Zoom command failed: {e}")
            raise

    async def _send_operation_with_data(
        self, op_code: int, params: list, payload: bytes
    ):
        """Send operation with data phase"""
        if not self.camera.connection.command_socket:
            raise Exception("Command socket not available")

        # Get current transaction ID and increment
        tx_id = self.camera.transaction_id
        self.camera.transaction_id += 1

        # Create all packets
        oper_req, start_data, data_packet, end_data = (
            create_operation_with_data_packets(op_code, tx_id, params, payload)
        )

        try:
            self.camera.connection.command_socket.send(oper_req)
            self.camera.connection.command_socket.send(start_data)
            self.camera.connection.command_socket.send(data_packet)
            self.camera.connection.command_socket.send(end_data)
            return 0x2001  # OK
        except Exception as e:
            logging.error(f"Zoom packet send failed: {e}")
            raise
