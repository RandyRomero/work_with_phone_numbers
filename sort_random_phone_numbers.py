import logging
import os
import typing as tp
from heapq import merge
from time import perf_counter

"""
This script sorts a file with random phone numbers or any other lines.

It sorts lines in chunks in order to avoid being killed by OS for consuming whole available RAM.

The strategy is to read a file with random phone numbers in chunks. Every chunk is sorted and 
stored on a disk. Then all chunks are merged into one resulting file.

You can choose size of chunks (to be precise, how many lines should be in one chunk) on your
own risk by providing environmental variable LINES_PER_CHUNK.
"""

logging.basicConfig(level=logging.INFO, format='%(asctime)s: %(message)s', datefmt='%H:%M:%S')
logger = logging.getLogger(__name__)

MILLION = 1_000_000

FILE_NAME_WITH_NUMBERS_TO_SORT = os.environ.get("FILE_WITH_NUMBERS_TO_SORT",
                                                "phone_numbers_shuffled.txt")

RESULTING_FILE_NAME = os.environ.get("RESULTING_FILE_NAME", "sorted_phone_numbers.txt")

LINES_PER_CHUNK = int(os.environ.get("LINES_PER_CHUNK", MILLION * 100))

CHUNK_NAME_TEMPLATE = os.environ.get("CHUNK_NAME_TEMPLATE", "chunk_{0}.txt")


def merge_files(chunk_file_names: tp.List[str], resulting_file_name: str) -> None:
    """Merge sorted files into one sorted file."""

    logger.info("Opening sorted files...")
    chunks = [open(file_name, "r") for file_name in chunk_file_names]

    logger.info("Merge chunks into one resulting file...")
    with open(resulting_file_name, "w") as outfile:
        outfile.writelines(merge(*chunks))

    [chunk.close() for chunk in chunks]


def split_huge_file_into_sorted_chucks(file_name_with_numbers_to_sort: str,
                                       lines_per_chunk: int,
                                       chunk_name_template: str) -> tp.List[str]:
    """Reads file line by line, splits it into chunks, sort lines in each chunk, saves it."""

    chunk_file_names = []

    with open(file_name_with_numbers_to_sort, "r") as phone_numbers_file:
        lines = []
        for i, line in enumerate(phone_numbers_file, start=1):
            lines.append(line)
            if i > 0 and i % lines_per_chunk == 0:
                file_part = int(i / lines_per_chunk)
                logger.info(f"Start sorting {file_part} part of original file...")
                lines.sort()
                file_name = chunk_name_template.format(file_part)
                chunk_file_names.append(file_name)
                with open(file_name, "w") as chunk:
                    logger.info(f"Saving {file_name}...")
                    chunk.writelines(lines)
                lines[:] = []

    return chunk_file_names


def main():
    """Controls main flow."""

    start_time = perf_counter()
    logger.info(f"Start sorting file: {FILE_NAME_WITH_NUMBERS_TO_SORT}")
    logger.info(f"Chunk size is {LINES_PER_CHUNK} lines.")
    logger.info(f"Resulting file name is {RESULTING_FILE_NAME} lines.")

    chunk_file_names = split_huge_file_into_sorted_chucks(FILE_NAME_WITH_NUMBERS_TO_SORT,
                                                          LINES_PER_CHUNK,
                                                          CHUNK_NAME_TEMPLATE)
    merge_files(chunk_file_names, RESULTING_FILE_NAME)

    [os.remove(file) for file in chunk_file_names]

    resulted_time = perf_counter() - start_time
    logger.info(f"Time taken for the whole script to run: {resulted_time} seconds")


if __name__ == '__main__':
    main()
