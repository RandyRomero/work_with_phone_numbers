import logging
import multiprocessing
import os
import typing as tp
from datetime import datetime
from random import shuffle
from time import perf_counter

"""
This script if for generating a file with a list of all numbers between +79000000000 and 
+79999999999 in a random order.

It optimized to be suitable for machines with ~12-16 Gb or RAM, that's why we don't create 
the whole list of numbers in the script right away. Instead, we create 10 files, distribute
numbers between them evenly, than shuffle every file and merge them into one final file.

Because of this split&merge strategy the script is far from the fast.
"""

START_NUMBER = int(os.environ.get("START_NUMBER", 79000000000))
STOP_NUMBER = int(os.environ.get("STOP_NUMBER", 80000000000))
TOTAL_ITERATIONS = STOP_NUMBER - START_NUMBER

SPLIT_FILE_NUMBER = int(os.environ.get("SPLIT_FILE_NUMBER", 10))
FINAL_FILE_NAME = os.environ.get("FINAL_FILE_NAME", "phone_numbers_shuffled.txt")

RAW_FILE_NAMES = [f"file_{i}.txt" for i in range(1, SPLIT_FILE_NUMBER+1)]

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


def generate_temp_files_with_numbers(start_number: int,
                                     stop_number: int,
                                     total_iterations: int,
                                     split_file_number: int,
                                     raw_files_name: tp.List[str]) -> None:
    """Generates several files with phone numbers."""

    files = [open(f"{raw_file_name}", "w") for raw_file_name in raw_files_name]

    start_time = perf_counter()

    logger.info(f"Start generating phone numbers at {datetime.now().strftime('%H:%M:%S')}")
    for number in range(start_number, stop_number):
        numbers_written = total_iterations - (stop_number - number)
        if numbers_written > 0 and numbers_written % 10000000 == 0:
            logger.info(f"There are {numbers_written} numbers has been written so far...")
            logger.info(f"Time since start: {perf_counter() - start_time} seconds")

        # I wanted to go with random.randrange() instead, but it turned out to be extremely slow.
        # Calling randrange() in loop like this adds ~12 seconds per 10 000 000 iterations.
        file_number = number % split_file_number
        files[file_number].write(f"+{number}\n")

    [file.close() for file in files]

    logger.info(f"Time taken to write 10 files with random numbers: {perf_counter() - start_time}")


def shuffle_one_file(file_name: str) -> str:
    """Opens file and shuffles its lines."""
    logger.info(f"Shuffling {file_name}...")
    start_shuffle_time = perf_counter()
    with open(file_name, 'r') as raw_file:
        lines = raw_file.readlines()
        shuffle(lines)
        logger.info(f"Time taken to shuffle {file_name}: {perf_counter() - start_shuffle_time}")

    start_writing_time = perf_counter()
    shuffled_file_name = f"shuffled_{file_name}"
    with open(shuffled_file_name, 'w') as shuffled_file:
        shuffled_file.writelines(lines)

    logger.info(f"Time taken to write {shuffled_file_name}: {perf_counter() - start_writing_time}")
    return shuffled_file_name


def shuffle_files(file_names: tp.Iterable[str]) -> tp.List[str]:
    """
    Shuffle files in loop one by one.

    This function spawns a child process, which will open and shuffle lines in the file.
    Why do we need a separate process for it? Because when it gets killed, the RAM is released, and
    we are good to go to open and shuffle the next file. Otherwise, the main process will consume
    all the memory and will be killed.
    """
    shuffled_file_names = []
    # processes=1 because we want to keep RAM consumption at a moderate level
    pool = multiprocessing.Pool(processes=1)
    for file_name in file_names:
        logger.info(f"Spawning a subprocess to shuffle {file_name}")
        logger.info("Wait for it to finish")
        result = pool.apply_async(shuffle_one_file, (file_name,))
        shuffled_file_names.append(result.get())

    [os.remove(file) for file in file_names]
    return shuffled_file_names


def merge_shuffled_files(shuffled_file_names: tp.List[str], final_file_name: str) -> None:
    """Merges files with phones into one file."""

    start_writing_time = perf_counter()

    logger.info("Opening final file...")
    with open(final_file_name, "w") as final_file:
        for file_name in shuffled_file_names:
            logger.info(f"Merging {file_name} to the final file...")
            with open(file_name, "r") as shuffled_file:
                final_file.write(shuffled_file.read())

    [os.remove(file) for file in shuffled_file_names]
    logger.info(f"Time taken to merge shuffled files: {perf_counter() - start_writing_time}")


def main():
    """Controls main flow."""
    start_time = perf_counter()

    generate_temp_files_with_numbers(start_number=START_NUMBER,
                                     stop_number=STOP_NUMBER,
                                     total_iterations=TOTAL_ITERATIONS,
                                     split_file_number=SPLIT_FILE_NUMBER,
                                     raw_files_name=RAW_FILE_NAMES)
    merge_shuffled_files(shuffle_files(RAW_FILE_NAMES), final_file_name=FINAL_FILE_NAME)

    resulted_time = perf_counter() - start_time
    logger.info(f"Time taken for the whole script to run: {resulted_time} seconds")


if __name__ == '__main__':
    main()
