"""
This function serves as a manager for the fetching scripts present in fetching_data:
1) Collects BTC data; the API handles up to 100,000 requests per month, 1 request is up to 2000 candles.
2) Collects general financial data; live data are taken by parsing TradingView alerts from my Gmail account.
3) Merges the live data with the historical one (taken from barchart/backtest market) at the start of the new hour (from system clock not script start time).
"""

import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import sys

# Define the directory path
log_dir = 'logging'

# Create the directory if it doesn't exist
os.makedirs(log_dir, exist_ok=True)

def setup_logger(name, log_file): # Sets up a logger with a RotatingFileHandler.

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()  # Clear existing handlers to avoid duplication

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

async def monitor_script(name, cmd, cwd, event):
    # Set up a logger for this script
    log_file = os.path.join(log_dir, f'{name}.log')
    logger = setup_logger(name, log_file)

    logger.info(f"Starting {name}")
    try:
        # Start the subprocess with the specified working directory
        process = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=cwd  # Set the working directory
        )

        async def read_stream(stream, log_level, stream_name):
            while True:
                line = await stream.readline()
                if not line:
                    if process.returncode is not None:
                        break  # Process has terminated
                    await asyncio.sleep(0.1)  # Yield control
                    continue
                try:
                    line = line.decode('utf-8').rstrip()
                except UnicodeDecodeError:
                    line = line.decode('utf-8', errors='replace').rstrip()
                print(f"{name} [{stream_name}]: {line}", flush=True)
                logger.log(log_level, f"{stream_name}: {line}")

                # Implement control logic here
                if "Waiting for the next minute..." in line:
                    logger.info(f"{name} reached the waiting state.")
                    # Signal that this script has reached the desired state
                    event.set()

        # Start reading streams
        stdout_task = asyncio.create_task(read_stream(process.stdout, logging.INFO, 'stdout'))
        stderr_task = asyncio.create_task(read_stream(process.stderr, logging.ERROR, 'stderr'))

        # Monitor the process without waiting indefinitely
        while True:
            if process.returncode is not None:
                logger.info(f"{name} has terminated with return code {process.returncode}.")
                break
            await asyncio.sleep(1)  # Adjust sleep duration as needed

        # Wait for the stream reading tasks to finish
        await stdout_task
        await stderr_task

    except Exception as e:
        logger.exception(f"Exception in {name}: {e}")

async def coordinate_processer(script_events):
    try:
        # Set up a logger for the processer
        log_file = os.path.join(log_dir, 'Processer.log')
        processer_logger = setup_logger('Processer', log_file)
        processer_logger.info("Processer logger set up successfully.")
        print("Processer logger set up successfully.", flush=True)

        while True:
            # Wait for all events to be set
            await asyncio.gather(*(event.wait() for event in script_events.values()))
            processer_logger.info("All scripts have reached the waiting state.")
            print("All scripts have reached the waiting state.", flush=True)

            # Launch processer.py
            processer_cmd = ['python', 'processer.py']
            processer_cwd = os.path.abspath(os.path.join('fetching_data', 'history', 'LIVE PROCESSED'))
            processer_logger.info(f"Launching processer.py in {processer_cwd}")
            print(f"Launching processer.py in {processer_cwd}", flush=True)

            try:
                processer_process = await asyncio.create_subprocess_exec(
                    *processer_cmd,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    cwd=processer_cwd  # Set the working directory
                )

                # Read processer.py output (both stdout and stderr)
                async def read_processer_output():
                    while True:
                        stdout_line = await processer_process.stdout.readline()
                        stderr_line = await processer_process.stderr.readline()

                        if not stdout_line and not stderr_line:
                            if processer_process.returncode is not None:
                                break
                            await asyncio.sleep(0.1)
                            continue

                        if stdout_line:
                            try:
                                line = stdout_line.decode('utf-8').rstrip()
                            except UnicodeDecodeError:
                                line = stdout_line.decode('utf-8', errors='replace').rstrip()
                            print(f"Processer [stdout]: {line}", flush=True)
                            processer_logger.info(f"stdout: {line}")

                        if stderr_line:
                            try:
                                line = stderr_line.decode('utf-8').rstrip()
                            except UnicodeDecodeError:
                                line = stderr_line.decode('utf-8', errors='replace').rstrip()
                            print(f"Processer [stderr]: {line}", flush=True)
                            processer_logger.error(f"stderr: {line}")

                # Start reading processer.py output
                output_task = asyncio.create_task(read_processer_output())

                # Wait for processer.py to complete
                await processer_process.wait()
                await output_task

                processer_logger.info("processer.py has completed.")
                print("processer.py has completed.", flush=True)

            except Exception as e:
                processer_logger.exception(f"Exception in processer.py: {e}")
                print(f"Exception when launching processer.py: {e}", flush=True)

            # Reset the events for the next cycle
            for event in script_events.values():
                event.clear()

            # Calculate time until the next hour
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            wait_seconds = (next_hour - now).total_seconds()
            processer_logger.info(f"Waiting for {wait_seconds:.0f} seconds until the next hour.")
            print(f"Waiting for {wait_seconds:.0f} seconds until the next hour.", flush=True)

            # Wait until the next hour
            await asyncio.sleep(wait_seconds)
    except Exception as e:
        print(f"Unhandled exception in coordinate_processer: {e}", flush=True)
        logging.exception(f"Unhandled exception in coordinate_processer: {e}")

async def main():
    # Base directory of your scripts
    base_dir = os.path.abspath(os.path.dirname(__file__))

    # List of scripts to run with their commands and working directories
    scripts = [
        ('TradingViewEmail_1min_tickers',
         ['python', '-u', 'TradingViewEmail_1min_tickers.py'],
         os.path.join(base_dir, 'live')),
        ('criptocompare_BTC_1m',
         ['python', '-u', 'criptocompare_BTC_1m.py'],
         os.path.join(base_dir, 'live')),
    ]

    # Create events for each script
    script_events = {name: asyncio.Event() for name, cmd, cwd in scripts}

    # Start monitoring all scripts concurrently
    monitor_tasks = [
        asyncio.create_task(monitor_script(name, cmd, cwd, script_events[name]))
        for name, cmd, cwd in scripts
    ]

    # Start the coordinator task
    coordinator_task = asyncio.create_task(coordinate_processer(script_events))

    # Wait for all tasks to complete (they won't unless scripts terminate)
    await asyncio.gather(*monitor_tasks, coordinator_task)

# Run the main coroutine
if __name__ == "__main__":
    # set the event loop policy for Windows (otherwise only one process work don't ask me why)
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
