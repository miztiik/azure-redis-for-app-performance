# -*- coding: utf-8 -*-

import os
import random
import string

import json
import logging
import datetime
import time
import socket

from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential

import redis

# ANSI color codes
GREEN_COLOR = "\033[32m"
RED_COLOR = "\033[31m"
RESET_COLOR = "\033[0m"

# Example usage with logging
logging.info(f'{GREEN_COLOR}This is green text{RESET_COLOR}')

class GlobalArgs:
    OWNER = "Mystique"
    VERSION = "2023-06-04"
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
    EVNT_WEIGHTS = {"success": 80, "fail": 20}
    TRIGGER_RANDOM_FAILURES = os.getenv("TRIGGER_RANDOM_FAILURES", True)
    WAIT_SECS_BETWEEN_MSGS = int(os.getenv("WAIT_SECS_BETWEEN_MSGS", 2))
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 1000))
    TEST_CYCLES = int(os.getenv("TEST_CYCLES", 500))

    REDIS_HOST = os.getenv("REDIS_HOST", "miztiik.redis.cache.windows.net")
    REDIS_PORT = os.getenv("REDIS_PORT", 6380)
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "yzj4RTwjieNM0nPOoxnxltB8LR9YWqLSTAzCaPMWNBM=")


    SA_NAME = os.getenv("SA_NAME", "redisaccnt")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL","https://redisaccnt.blob.core.windows.net")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events")
    BLOB_PREFIX = "store-events"



def main():
    r = redis.StrictRedis(
        host=GlobalArgs.REDIS_HOST, 
        port=GlobalArgs.REDIS_PORT,
        ssl=True,
        password=GlobalArgs.REDIS_PASSWORD,
        db=0
        )

    keys = []
    i = 0
    rr = "order"
    c_miss = 0


    while i < int(GlobalArgs.TEST_CYCLES):
        # Randomize file seek
        _seeker = random.randrange(1, GlobalArgs.TOT_MSGS_TO_PRODUCE)
        print(f"seeking {rr}-{_seeker}")

        # Start timer
        start = datetime.datetime.now()

        value = r.get(f"{rr}-{_seeker}")
        if value is None:
            c_miss += 1
        print(value)

        end = datetime.datetime.now()
        # End timer

        delta = end - start
        millis = delta.seconds * 1000000
        millis += delta.microseconds
        keys.append(millis)
        i += 1

    # throw out first request due to initialization overhead
    keys.pop(0)

    # print timing
    sum = 0
    for idx, timing in enumerate(keys):
        sum += timing
        # uncomment below to see timing for each request
        # print(f'Time of request:{idx} is {timing}')


    print('=====Timing=====\n')

    average = sum / len(keys)
    print(f'Average Latency in Microseconds: {average}')
    print(f'MAX Latency in Microseconds: {max(keys)}')
    print(f'MIN Latency in Microseconds: {min(keys)}')
    print(f'Cache Misses: {c_miss} out of {GlobalArgs.TOT_MSGS_TO_PRODUCE}')
    print(f'\nCompleted Successfully!')

# Run the whole file
if __name__ == "__main__":
    main()

