import asyncio
import logging
import time

from sony_camera import SonyCamera
from sony_camera.protocol import SONY_PROPERTIES

CAM_IP = "192.168.1.47"
CAM_USERNAME = "admin"
CAM_PASSWORD = "Aa123456"


async def main():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    camera = SonyCamera(CAM_IP, CAM_USERNAME, CAM_PASSWORD)

    try:
        success = await camera.connect()
        if success:
            logging.info("✅ Camera connected successfully!")

            # Wait for camera to be ready
            max_wait = 15
            waited = 0
            while not camera.sdio_ready and waited < max_wait:
                await asyncio.sleep(0.5)
                waited += 0.5

            await camera.ensure_sdio_ready()

            logging.info("Camera ready!")

            all_prop = camera.get_all_properties_sync()

            logging.info(f"All properties dict: {all_prop}")

            for f_value in [
                100,
                110,
                120,
                130,
                140,
                160,
                170,
                180,
                200,
                220,
                240,
                250,
                280,
                320,
                350,
                400,
                450,
                500,
                560,
                630,
                670,
                710,
                800,
                900,
                950,
                1000,
                1100,
                1300,
                1400,
                1600,
                1800,
                1900,
                2000,
                2200,
                2500,
                2700,
                2900,
                3200,
                3600,
                3800,
                4000,
                4500,
                5100,
                5400,
                5700,
                6400,
                7200,
                7600,
                8100,
                9000,
            ]:
                camera.set_device_property_sync(SONY_PROPERTIES["F_NUMBER"], f_value)
                logging.info(f"Set F_NUMBER to {f_value}")
                await asyncio.sleep(delay=1)

        else:
            logging.error("❌ Connection failed")
    except KeyboardInterrupt:
        logging.info("Exiting...")
    except Exception as e:
        logging.error(f"Error: {e}")
    finally:
        await camera.disconnect()
        logging.info("Disconnected from camera")


if __name__ == "__main__":
    asyncio.run(main())
