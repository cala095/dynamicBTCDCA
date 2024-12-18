import asyncio
import logging
import os
from logging.handlers import RotatingFileHandler
from datetime import datetime, timedelta
import sys
import time

# -------------------------------------
# Configuration
# -------------------------------------
log_dir = '../fetching_data/logging'
os.makedirs(log_dir, exist_ok=True)

# This is the log file we will read from to get "Waiting for X seconds"
# Adjust this as needed. The script assumes this log is continuously updated.
LOG_FILE_TO_CHECK = os.path.join(log_dir, 'Processer.log')

# Scripts and their commands
DUPLICATE_CHECKER = ('duplicate_checker', ['python', '-u', 'duplicate_checker.py'], '../vectoring_data/timing') # cwd is current working directory, so that each script has his own
MISSINGMINUTE_CHECKER = ('missingMinute_checker', ['python', '-u', 'missingMinute_checker.py'], '../vectoring_data/timing')

UPLOAD_BACKUP_DATA = ('upload_backup_data', ['python', '-u', 'upload_backup_data.py'], '../cloud_deploy')

TIME_PROCESSER = ('time_processer', ['python', '-u', 'time_processer.py'], '../vectoring_data/timing')
INDICATORS_PROCESSER = ('indicators_processer', ['python', '-u', 'indicators_processer.py'], '../vectoring_data/indicators')
ENV_DATA_PREP = ('environment_data_prep', ['python', '-u', 'environment-data-prep.py'], '../environment_gym/data')
ENV_PROD = ('environment_prod', ['python', '-u', 'environment-prod.py'], '../environment_gym/data')

# Threshold in seconds
MINIMUM_SECONDS_THRESHOLD = 2000

def setup_logger(name, log_file):
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    handler = RotatingFileHandler(
        log_file,
        maxBytes=10 * 1024 * 1024,
        backupCount=5
    )
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.propagate = False
    return logger

main_logger = setup_logger('Main', os.path.join(log_dir, 'cloud_data_distribution.log'))

async def run_script(name, cmd, cwd='.', check_output=False, expected_output=None):
    main_logger.info(f"Starting {name} with command: {cmd} in directory: {cwd}")
    print(f"Starting {name}...", flush=True)

    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd
    )

    stdout_data = []
    stderr_data = []

    async def read_stream(stream, log_func, storage, prefix):
        # This loop ends when the process closes the stream (i.e., on termination).
        async for line_bytes in stream:
            decoded_line = line_bytes.decode('utf-8', errors='replace').rstrip()
            storage.append(decoded_line)
            log_func(f"{name} [{prefix}]: {decoded_line}")
            print(f"{name} [{prefix}]: {decoded_line}", flush=True)

    # Create tasks to read stdout and stderr concurrently
    stdout_task = asyncio.create_task(read_stream(process.stdout, main_logger.info, stdout_data, 'stdout'))
    stderr_task = asyncio.create_task(read_stream(process.stderr, main_logger.error, stderr_data, 'stderr'))

    # Wait for both reading tasks to complete
    await asyncio.gather(stdout_task, stderr_task)

    # Now wait for the process to fully finish
    returncode = await process.wait()

    if returncode != 0:
        main_logger.error(f"{name} ended with return code {returncode}.")
        raise Exception(f"{name} failed with return code {returncode}")

    if check_output and expected_output:
        if not any(expected_output in line for line in stdout_data):
            main_logger.error(f"{name} did not print the expected output '{expected_output}'")
            raise Exception(f"{name} did not print the expected output '{expected_output}'")

    main_logger.info(f"{name} completed successfully.")
    print(f"{name} completed successfully.", flush=True)
    return True


def get_last_two_lines(log_file):
    """
    Read the last two lines of a log file.
    """
    if not os.path.exists(log_file):
        return []

    with open(log_file, 'rb') as f:
        # Move to end
        f.seek(0, os.SEEK_END)
        file_size = f.tell()
        block_size = 1024
        data = b''
        while True:
            if file_size < block_size:
                block_size = file_size
            file_size -= block_size
            f.seek(file_size, os.SEEK_SET)
            block = f.read(block_size)
            data = block + data
            if data.count(b'\n') >= 2 or file_size == 0:
                break

        lines = data.decode('utf-8', errors='replace').split('\n')
        # Strip empty lines at the end
        lines = [l for l in lines if l.strip()]
        return lines[-2:] if len(lines) >= 2 else lines

def parse_log_lines(lines):
    """
    Given two lines that look like:
    2024-12-06 21:06:06,523 - INFO - processer.py has completed.
    2024-12-06 21:06:06,523 - INFO - Waiting for 3233 seconds until the next hour.

    Parse the date/time and the seconds until next hour.
    Return:
    (timestamp_of_line, seconds_until_next_hour)

    timestamp_of_line is the datetime parsed from the line timestamp.
    seconds_until_next_hour is the integer number of seconds parsed from the second line.
    """
    if len(lines) < 2:
        return None, None

    # First line: we just parse the timestamp
    # Format example: 2024-12-06 21:06:06,523 - INFO - ...
    def parse_timestamp(line):
        # Timestamp format: YYYY-MM-DD HH:MM:SS,mmm
        # Let's split by ' - ' to isolate the timestamp
        # line.split(' - ', 1) might help but we know the format:
        # "YYYY-MM-DD HH:MM:SS,mmm - INFO - ..."
        date_str = line.split(' - ')[0]
        # Example date_str: "2024-12-06 21:06:06,523"
        # Parse with datetime
        return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S,%f")

    first_ts = parse_timestamp(lines[0])
    second_ts = parse_timestamp(lines[1])

    # Second line should contain: "Waiting for XXXX seconds until the next hour."
    # Let's extract that:
    # Example second line: "2024-12-06 21:06:06,523 - INFO - Waiting for 3233 seconds until the next hour."
    # Split by 'Waiting for ' and then split by ' seconds'
    second_line_content = lines[1]
    try:
        after_waiting_for = second_line_content.split('Waiting for ')[1]
        seconds_part = after_waiting_for.split(' seconds')[0]
        seconds_until_next_hour = int(seconds_part.strip())
    except:
        # Could not parse
        seconds_until_next_hour = None

    # We return the timestamp of the second line (which is the one with waiting info)
    return second_ts, seconds_until_next_hour

async def main():
    try:
        # 1) Run duplicate_checker and missingMinute_checker in parallel and check their outputs
        await asyncio.gather(
            run_script(*DUPLICATE_CHECKER, check_output=True, expected_output="STATUS OK"),
            run_script(*MISSINGMINUTE_CHECKER, check_output=True, expected_output="STATUS OK")
        )

        # If both STATUS OK, then run upload_backup_data
        await run_script(*UPLOAD_BACKUP_DATA, check_output=True, expected_output="STATUS OK")

        # 2) Enter the loop
        while True:
            # Read the last two lines from the log file
            lines = get_last_two_lines(LOG_FILE_TO_CHECK)

            # If we don't have two lines or can't parse them, wait a bit and retry
            if len(lines) < 2:
                main_logger.warning("Not enough log lines to parse. Will wait 10 seconds.")
                await asyncio.sleep(10)
                continue

            log_ts, seconds_until_next_hour = parse_log_lines(lines)
            if not log_ts or not seconds_until_next_hour:
                main_logger.warning("Could not parse the log lines properly. Waiting 10 seconds.")
                await asyncio.sleep(10)
                continue

            # Check if the log is stale:
            # Condition: we must be in the same day and hour as the current time to proceed.
            now = datetime.now()
            same_day_and_hour = (log_ts.year == now.year and
                                 log_ts.month == now.month and
                                 log_ts.day == now.day and
                                 log_ts.hour == now.hour)

            if not same_day_and_hour:
                # Not same day/hour, check every 10 seconds for updates
                main_logger.info("Log timestamp not in the same day/hour. Waiting 10 seconds.")
                await asyncio.sleep(10)
                continue

            # If we are in the same day/hour, check if we have >= 2000 seconds until next hour
            if seconds_until_next_hour < MINIMUM_SECONDS_THRESHOLD:
                # If we don't have enough margin, throw error and exit
                main_logger.error(f"Not enough time margin. Only {seconds_until_next_hour} seconds left until next hour.")
                print("ERROR: Not enough time margin. Exiting.", flush=True)
                sys.exit(1)

            # If we reached this point:
            # We have the same day/hour and more than 2000 seconds until next hour
            # Proceed with the sequence:
            await run_script(*TIME_PROCESSER, check_output=True, expected_output="STATUS OK")
            await run_script(*INDICATORS_PROCESSER, check_output=True, expected_output="STATUS OK")
            await run_script(*ENV_DATA_PREP, check_output=True, expected_output="STATUS OK")
            # await run_script(*ENV_PROD) -> we will lunch this on his own
            #TODO add the upload script
            await run_script(*UPLOAD_BACKUP_DATA, check_output=True, expected_output="STATUS OK")

            # Wait for the next hour before restarting the loop
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            wait_seconds = (next_hour - now).total_seconds()
            main_logger.info(f"Sequence completed. Waiting {wait_seconds:.0f} seconds until the next hour.")
            print(f"Waiting {wait_seconds:.0f} seconds until the next hour before next loop...", flush=True)
            await asyncio.sleep(wait_seconds)

    except Exception as e:
        main_logger.exception(f"Unhandled exception in main: {e}")
        print(f"Unhandled exception: {e}", flush=True)
        sys.exit(1)


if __name__ == "__main__":
    if sys.platform == 'win32':
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    asyncio.run(main())
