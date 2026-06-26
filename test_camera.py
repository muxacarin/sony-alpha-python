#!/usr/bin/env python3
"""
Simple test script for Sony Camera control
"""

import asyncio
import logging
import sys
import time

from sony_camera import SonyCamera


async def main():
    """Test the Sony camera connection and zoom"""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    if len(sys.argv) >= 4:
        ip = sys.argv[1]
        username = sys.argv[2]
        password = sys.argv[3]
    else:
        logging.error("Usage: python test_camera.py <ip> <username> <password>")
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
                await camera.start_zoom("in", 2)
                time.sleep(2)
                await camera.stop_zoom()

                time.sleep(1)

                await camera.start_zoom("out", 2)
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
    asyncio.run(main())
