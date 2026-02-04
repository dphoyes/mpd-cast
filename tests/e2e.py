
import logging
import sys
import anyio
import subprocess
import os
from mpdserver import mpdclient
from zeroconf import Zeroconf
import pychromecast
from mpd_cast.mpd import discover_chromecasts

from . import e2e_config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_val(data, key):
    val = data.get(key) or data.get(key.encode('utf-8'))
    return val.decode('utf-8') if isinstance(val, bytes) else val

async def wait_for_status(chromecast_device, expected_status):
    logger.info(f"Waiting for status: {expected_status}...")
    try:
        with anyio.fail_after(20):
            while True:
                if chromecast_device.media_controller.status.player_state == expected_status:
                    logger.info(f"SUCCESS: Chromecast is {expected_status}!")
                    return
                await anyio.sleep(0.5)
    except TimeoutError:
        sys.exit(f"Verification failed: Device not {expected_status}.")

async def main():
    logger.info("Starting mpd-cast subprocess...")
    # Find mpd-cast in the same bin directory as the python executable
    mpd_cast_bin = os.path.join(sys.prefix, 'bin', 'mpd-cast')
    cmd = [
        mpd_cast_bin, "-p", "6600",
        "--db", f"{e2e_config.MPD_HOST}:{e2e_config.MPD_PORT}",
        "--app-id", e2e_config.APP_ID,
        "--web-host", e2e_config.WEB_HOST,
        "--partition-prefix", e2e_config.PARTITION_PREFIX
    ]
    env = os.environ.copy()
    env['PYTHONTRACEMALLOC'] = '1'
    proc = subprocess.Popen(cmd, stdout=sys.stdout, stderr=sys.stderr, env=env)

    try:
        # Wait for server
        try:
            with anyio.fail_after(20):
                while True:
                    try:
                        async with mpdclient.MpdClient('localhost', 6600):
                            break
                    except* OSError:
                        await anyio.sleep(1)
        except TimeoutError:
            sys.exit("Could not connect to mpd-cast server.")

        logger.info("Connected. Finding physical device...")
        zconf = Zeroconf()
        chromecast_device = None
        
        async for change, info in discover_chromecasts(zconf):
            if change in ('+', '-+') and info.friendly_name == e2e_config.TARGET_CHROMECAST_NAME:
                chromecast_device = await anyio.to_thread.run_sync(
                    pychromecast.get_chromecast_from_cast_info, info, zconf
                )
                await anyio.to_thread.run_sync(chromecast_device.wait)
                logger.info(f"Found physical device: {info.friendly_name}")
                break

        logger.info("Finding output in MPD...")
        
        async with mpdclient.MpdClient('localhost', 6600) as client:
            target_oid = None
            try:
                with anyio.fail_after(120):
                    while not target_oid:
                        outputs = await client.command_returning_raw_objects(b"outputs", delimiter=b"outputid")
                        for output in outputs:
                            if get_val(output, 'outputname') == e2e_config.TARGET_CHROMECAST_NAME:
                                target_oid = get_val(output, 'outputid')
                                break
                        if not target_oid:
                            await anyio.sleep(1)
            except TimeoutError:
                sys.exit(f"Timeout waiting for Chromecast '{e2e_config.TARGET_CHROMECAST_NAME}'.")

            logger.info(f"Found output {target_oid}. Setting up playback...")
            await client.command_returning_nothing(f"enableoutput {target_oid}".encode('utf-8'))
            await client.command_returning_nothing(b"clear")
            await client.command_returning_nothing(f"load \"{e2e_config.TEST_PLAYLIST}\"".encode('utf-8'))
            await client.command_returning_nothing(b"play")

            # 1. Verify initial playback
            await wait_for_status(chromecast_device, 'PLAYING')
            
            # 2. Pause
            logger.info("Sending 'pause 1'...")
            await client.command_returning_nothing(b"pause 1")
            await wait_for_status(chromecast_device, 'PAUSED')

            # 3. Resume with 'play 5'
            logger.info("Sending 'play 5'...")
            await client.command_returning_nothing(b"play 5")
            await wait_for_status(chromecast_device, 'PLAYING')
            
    finally:
        if chromecast_device:
            logger.info("Quitting Chromecast app...")
            await anyio.to_thread.run_sync(chromecast_device.quit_app)

        logger.info("Terminating mpd-cast...")
        proc.terminate()
        proc.wait()

if __name__ == "__main__":
    try:
        anyio.run(main)
    except KeyboardInterrupt:
        pass
