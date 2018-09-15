# s3-copy-concurrent

This is a Python 3 script that provides a function to concurrenrly copy files from one location in AWS S3 to another. Concurrent copy operations on multiple directories expedites copy times. When running, it prints:

    364_1 -> 37303
    Folder doesnt exist
    original/364/1 moves to new/37303
    Copying 23490 items using <function copy at 0x11eeb2c80> in 32 processes.
    23490 copy operations in 1:19

The first line means that we are checking the files under the prefix 364_1 to ensure they are all in 37303. Since 37303 doesn't exist, we copy from original/364/1 to new/37303. This runs in 32 processes, i.e. 32 concurrent copy operations, and completes in 1 minute and 19 seconds.

The only dependency is the AWS s3 library `boto3` which can be installed into your `venv` with `pip install boto3`.