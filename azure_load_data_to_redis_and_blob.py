# -*- coding: utf-8 -*-

import os
import random
import string
import uuid


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
    TOT_MSGS_TO_PRODUCE = int(os.getenv("TOT_MSGS_TO_PRODUCE", 500))

    REDIS_HOST = os.getenv("REDIS_HOST", "miztiik.redis.cache.windows.net")
    REDIS_PORT = os.getenv("REDIS_PORT", 6380)
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "yzj4RTwjieNM0..LSTAzCaPMWNBM=")


    SA_NAME = os.getenv("SA_NAME", "redisaccnt")
    BLOB_SVC_ACCOUNT_URL = os.getenv("BLOB_SVC_ACCOUNT_URL","https://redisaccnt.blob.core.windows.net")
    BLOB_NAME = os.getenv("BLOB_NAME", "store-events")
    BLOB_PREFIX = "store-events"


def random_str_generator(size=40, chars=string.ascii_uppercase + string.digits):
    ''' Generate Random String for given string length '''
    return ''.join(random.choice(chars) for _ in range(size))

def _rand_coin_flip():
    r = False
    if GlobalArgs.TRIGGER_RANDOM_FAILURES:
        r = random.choices([True, False], weights=[0.1, 0.9], k=1)[0]
    return r

def _gen_uuid():
    return str(uuid.uuid4())

def generate_event():

    # Following Patterns are implemented
    # If event_type is inventory_event, then is_return is True for 50% of the events
    # 10% of total events are poison pill events, bad_msg attribute is True and store_id is removed
    # Event attributes are set with priority_shipping, is_return, and event type


    _categories = ["Books", "Games", "Mobiles", "Groceries", "Shoes", "Stationaries", "Laptops", "Tablets", "Notebooks", "Camera", "Printers", "Monitors", "Speakers", "Projectors", "Cables", "Furniture"]
    _variants = ["black", "red"]
    _evnt_types = ["sale_event", "inventory_event"]
    _payments = ["credit_card", "debit_card", "cash", "wallet", "upi", "net_banking", "cod", "gift_card"]

    _qty = random.randint(1, 99)
    _s = round(random.random() * 100, 2)

    _evnt_type = random.choices(_evnt_types, weights=[0.8, 0.2], k=1)[0]
    _u = _gen_uuid()
    p_s = random.choices([True, False], weights=[0.3, 0.7], k=1)[0]
    is_return = False

    if _evnt_type == "inventory_event":
        is_return = bool(random.getrandbits(1))

    evnt_body = {
        "id": _u,
        # "event_type": _evnt_type,
        "store_id": random.randint(1, 10),
        "store_fqdn": str(socket.getfqdn()),
        "store_ip": str(socket.gethostbyname(socket.gethostname())),
        "cust_id": random.randint(100, 999),
        "category": random.choice(_categories),
        "sku": random.randint(18981, 189281),
        "price": _s,
        "qty": _qty,
        "discount": random.randint(0, 75),
        "gift_wrap": random.choices([True, False], weights=[0.3, 0.7], k=1)[0],
        "variant": random.choice(_variants),
        "priority_shipping": p_s,
        "payment_method": random.choice(_payments),
        "ts": datetime.datetime.now().isoformat(),
        "contact_me": "github.com/miztiik",
        "is_return": is_return
    }

    if _rand_coin_flip():
        evnt_body.pop("store_id", None)
        evnt_body["bad_msg"] = True

    _attr = {
        "event_type": _evnt_type,
        "priority_shipping": str(p_s),
        "is_return": str(is_return)
    }

    return evnt_body, _attr

def evnt_producer():
    resp = {
        "status": False,
        "tot_msgs": 0
    }
    rr = "order"
    try:
        t_msgs = 0
        p_cnt = 0
        s_evnts = 0
        inventory_evnts = 0
        t_sales = 0

        while t_msgs < GlobalArgs.TOT_MSGS_TO_PRODUCE:
            evnt_body, evnt_attr = generate_event()
            logging.info(f"generated_event: {json.dumps(evnt_body)}")

            t_msgs += 1
            t_sales += evnt_body["price"] * evnt_body["qty"]

            if evnt_body.get("bad_msg"):
                p_cnt += 1

            if evnt_attr["event_type"] == "sale_event":
                s_evnts += 1
            elif evnt_attr["event_type"] == "inventory_event":
                inventory_evnts += 1

            time.sleep(GlobalArgs.WAIT_SECS_BETWEEN_MSGS)
            logging.info(f"generated_event:{json.dumps(evnt_body)}")

            azure_log_level = logging.getLogger("azure").setLevel(logging.ERROR)
            default_credential = DefaultAzureCredential(logging_enable=False,logging=azure_log_level)

            blob_svc_client = BlobServiceClient(GlobalArgs.BLOB_SVC_ACCOUNT_URL, credential=default_credential, logging=azure_log_level)
            
            # write to blob
            _evnt_type=evnt_attr["event_type"]
            print(f"Writing to blob: {rr}-{t_msgs}")
            write_to_blob(f"{rr}-{t_msgs}", evnt_body, blob_svc_client)

            # Write to Redis
            redis_client = redis.StrictRedis(
                host=GlobalArgs.REDIS_HOST, 
                port=GlobalArgs.REDIS_PORT,
                ssl=True,
                password=GlobalArgs.REDIS_PASSWORD,
                db=0
                )
            write_to_redis(redis_client, f"{rr}-{t_msgs}", evnt_body)
        
        resp["tot_msgs"] = t_msgs
        resp["bad_msgs"] = p_cnt
        resp["sale_evnts"] = s_evnts
        resp["inventory_evnts"] = inventory_evnts
        resp["tot_sales"] = t_sales
        resp["status"] = True

    except Exception as e:
        logging.error(f"ERROR: {str(e)}")
        resp["err_msg"] = str(e)

    return resp

def write_to_blob1(blob_name, data: dict, blob_svc_client):
    try:
        resp = blob_svc_client.get_blob_client(container=f"{GlobalArgs.BLOB_NAME}", blob=blob_name).upload_blob(json.dumps(data).encode("UTF-8"))
        logging.info(f"Blob {GREEN_COLOR}{blob_name}{RESET_COLOR} uploaded successfully")
        logging.debug(f"{resp}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

def write_to_blob(blob_name, data: dict, blob_svc_client):
    try:
        container_name = f"{GlobalArgs.BLOB_NAME}"
        container_client = blob_svc_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(blob_name)
        
        if blob_client.exists():
            blob_client.delete_blob()
            logging.info(f"Blob {GREEN_COLOR}{blob_name}{RESET_COLOR} deleted successfully")
        
        resp = blob_client.upload_blob(json.dumps(data).encode("UTF-8"))
        logging.info(f"Blob {GREEN_COLOR}{blob_name}{RESET_COLOR} uploaded successfully")
        logging.debug(f"{resp}")
    except Exception as e:
        logging.exception(f"ERROR:{str(e)}")

def write_to_redis(redis_client, key, value):
    ''' Write to Redis '''
    _r = redis_client.set(key, str(value))
    logging.info(f"Insered to Redis: {key}")
    logging.debug(f"{_r}")


def load_blob_and_cache():
    _d={
        "miztiik_event_processed": False,
        "msg": ""
    }

    resp = evnt_producer()
    _d["resp"] = resp
    if resp.get("status"):
        _d["miztiik_event_processed"] = True
        _d["msg"] = f"Generated {resp.get('tot_msgs')} messages"
        _d["count"] = GlobalArgs.TOT_MSGS_TO_PRODUCE
        _d["last_processed_on"] = datetime.datetime.now().isoformat()
    logging.info(f"{GREEN_COLOR} {json.dumps(_d)} {RESET_COLOR}")



if __name__ == "__main__":
    load_blob_and_cache()