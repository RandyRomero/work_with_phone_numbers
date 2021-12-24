# work_with_phone_numbers

Test assignment.

There are two scripts.
One generates a file with random phone numbers from +79000000000 to +79999999999 in a random
order.
The second takes this file, sort phone numbers and writes them in ascending order to a new file.

Both scripts optimized to use not more than 16 Gb of RAM, otherwise it does not work on my machine.
Thereby, both scripts do their work in chunks, which makes them not blazingly fast.

Tested on Python 3.8.

No external dependencies required.