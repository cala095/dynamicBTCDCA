"""
This function serves as a manager for the fetching scripts present in fetching_data:
1) Collects BTC data; the API handles up to 100,000 requests per month, 1 request is up to 2000 candles.
2) Collects general financial data; live data are taken by parsing TradingView alerts from my Gmail account.
3) Merges the live data with the historical one (taken from barchart/backtest market).
"""

import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta

# Define the directory path
log_dir = 'logging'  # Corrected 'loggin' to 'logging' for consistency

# Create the directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

def setup_logger(name, log_file):
    """
    Sets up a logger with a RotatingFileHandler.

    Parameters:
        name (str): The name of the logger.
        log_file (str): The file path for the log file.

    Returns:
        logging.Logger: Configured logger.
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Check if the logger already has handlers to avoid duplicate logs
    if not logger.handlers:
        handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5  # Keep up to 5 backup files
        )
        handler.setLevel(logging.INFO)

        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)

        logger.addHandler(handler)
        logger.propagate = False

    return logger

async def monitor_script(name, cmd, event):
    # Set up a logger for this script
    log_file = os.path.join(log_dir, f'{name}.log')
    logger = setup_logger(name, log_file)

    logger.info(f"Starting {name}")
    try:
        # Start the subprocess
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )

        async def read_stream(stream, log_level, stream_name):
            while True:
                line = await stream.readline()
                if not line:
                    break
                decoded_line = line.decode().rstrip()
                print(f"{name} [{stream_name}]: {decoded_line}")
                logger.log(log_level, f"{stream_name}: {decoded_line}")

                # Implement control logic here
                if "Waiting for the next minute..." in decoded_line:
                    logger.info(f"{name} reached the waiting state.")
                    # Signal that this script has reached the desired state
                    event.set()

        # Create tasks for reading stdout and stderr
        stdout_task = asyncio.create_task(read_stream(process.stdout, logging.INFO, 'stdout'))
        stderr_task = asyncio.create_task(read_stream(process.stderr, logging.ERROR, 'stderr'))

        # Wait for the process to complete
        await process.wait()

        # Wait for the stream reading tasks to complete
        await stdout_task
        await stderr_task

        logger.info(f"{name} has terminated.")

    except Exception as e:
        logger.exception(f"Exception in {name}: {e}")

async def coordinate_processer(script_events):
    # Set up a logger for the processer
    log_file = os.path.join(log_dir, 'Processer.log')
    processer_logger = setup_logger('Processer', log_file)

    while True:
        # Wait for all events to be set
        await asyncio.gather(*(event.wait() for event in script_events.values()))
        processer_logger.info("All scripts have reached the waiting state.")

        # Launch processer.py
        processer_cmd = ['python', os.path.join('fetching_data', 'history', 'LIVE PROCESSED', 'processer.py')]
        processer_logger.info("Launching processer.py")
        try:
            processer_process = await asyncio.create_subprocess_exec(
                *processer_cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # Read processer.py output (optional)
            async def read_processer_output():
                while True:
                    line = await processer_process.stdout.readline()
                    if not line:
                        break
                    decoded_line = line.decode().rstrip()
                    print(f"Processer [stdout]: {decoded_line}")
                    processer_logger.info(f"stdout: {decoded_line}")

            # Start reading processer.py output
            output_task = asyncio.create_task(read_processer_output())

            # Wait for processer.py to complete
            await processer_process.wait()
            await output_task

            processer_logger.info("processer.py has completed.")

        except Exception as e:
            processer_logger.exception(f"Exception in processer.py: {e}")

        # Reset the events for the next cycle
        for event in script_events.values():
            event.clear()

        # Calculate time until the next hour
        now = datetime.now()
        next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
        wait_seconds = (next_hour - now).total_seconds()
        processer_logger.info(f"Waiting for {wait_seconds:.0f} seconds until the next hour.")

        # Wait until the next hour
        await asyncio.sleep(wait_seconds)

async def main():
    # List of scripts to run
    scripts = [
        ('Script1', ['python', os.path.join('live', 'TradingViewEmail_1min_tickers.py')]),
        ('Script2', ['python', os.path.join('live', 'criptocompare_BTC_2012UPD.py')]),
    ]

    # Create events for each script
    script_events = {name: asyncio.Event() for name, cmd in scripts}

    # Start monitoring all scripts concurrently
    monitor_tasks = [
        asyncio.create_task(monitor_script(name, cmd, script_events[name]))
        for name, cmd in scripts
    ]

    # Start the coordinator task
    coordinator_task = asyncio.create_task(coordinate_processer(script_events))

    # Wait for all tasks to complete (they won't unless scripts terminate)
    await asyncio.gather(*monitor_tasks, coordinator_task)

# Run the main coroutine
if __name__ == "__main__":
    asyncio.run(main())
