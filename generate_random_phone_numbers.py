import logging
import os
import typing as tp
from datetime import datetime
from random import shuffle, random
from time import perf_counter

"""
This script if for generating a file with a list of all numbers between +79000000000 and 
+79999999999 in a random order.

It optimized to be suitable for machines with ~12-16 Gb or RAM, that's why we don't create 
the whole list of numbers in the script right away. Instead, we create 10 files, distribute
numbers between them, than shuffle every file and merge them into one final file.

Because of this split&merge strategy the script is not that fast.
"""

START_NUMBER = int(os.environ.get("START_NUMBER", 79000000000))
STOP_NUMBER = int(os.environ.get("STOP_NUMBER", 80000000000))
TOTAL_ITERATIONS = STOP_NUMBER - START_NUMBER

NUMBER_OF_CHUNKS = int(os.environ.get("NUMBER_OF_CHUNKS", 20))
FINAL_FILE_NAME = os.environ.get("FINAL_FILE_NAME", "phone_numbers_shuffled.txt")

RAW_FILE_NAMES = tuple(f"file_{i}.txt" for i in range(1, NUMBER_OF_CHUNKS+1))

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)


def generate_temp_files_with_numbers(start_number: int,
                                     stop_number: int,
                                     total_iterations: int,
                                     number_of_chunks: int,
                                     raw_files_name: tp.Tuple[str]) -> None:
    """Generates several files with phone numbers."""

    files = [open(f"{raw_file_name}", "w") for raw_file_name in raw_files_name]

    start_time = perf_counter()

    logger.info(f"Start generating phone numbers at {datetime.now().strftime('%H:%M:%S')}")
    for number in range(start_number, stop_number):
        numbers_written = total_iterations - (stop_number - number)
        if numbers_written > 0 and numbers_written % 10000000 == 0:
            logger.info(f"There are {numbers_written} numbers has been written so far...")
            logger.info(f"Time since start: {perf_counter() - start_time} seconds")

        file_number = int(random() * number_of_chunks + 1)
        files[file_number].write(f"+{number}\n")

    [file.close() for file in files]

    logger.info(f"Time taken to write 10 files with random numbers: {perf_counter() - start_time}")


def shuffle_and_merge(file_names: tp.Tuple[str], final_file_name: str) -> None:
    """Opens a chunk with numbers, shuffles, writes them out to the resulting file."""
    start_writing_time = perf_counter()
    logger.info("Opening final file...")

    with open(final_file_name, "w") as final_file:
        for file_name in file_names:
            with open(file_name, 'r') as raw_file:
                logger.info(f"Shuffling {file_name}...")
                lines = raw_file.readlines()
                shuffle(lines)
                final_file.writelines(lines)

    [os.remove(file) for file in file_names]
    logger.info(f"Time taken to shuffle and merge lines: {perf_counter() - start_writing_time}")


def main():
    """Controls main flow."""
    start_time = perf_counter()

    generate_temp_files_with_numbers(start_number=START_NUMBER,
                                     stop_number=STOP_NUMBER,
                                     total_iterations=TOTAL_ITERATIONS,
                                     number_of_chunks=NUMBER_OF_CHUNKS,
                                     raw_files_name=RAW_FILE_NAMES)
    shuffle_and_merge(file_names=RAW_FILE_NAMES, final_file_name=FINAL_FILE_NAME)

    resulted_time = perf_counter() - start_time
    logger.info(f"Time taken for the whole script to run: {resulted_time} seconds")


if __name__ == '__main__':
    main()
