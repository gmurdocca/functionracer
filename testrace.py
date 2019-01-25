#!/usr/bin/env python
import concurrent_callers
import logging
import random
import time
import sys
import os

logging.getLogger().setLevel(logging.NOTSET)

def phonyworker(t1, t2, exception_probability=0):
    """
    sleeps for random time between t2 - t1 seconds.
    Raises Exception `exception_probability`*100% of the time
    """
    assert t2 >= t1
    assert 0 <= exception_probability <= 1

    sleeptime = random.randint(t1, t2)
    #logging.info(f"Sleeping {sleeptime}s ({exception_probability*100}% chance of Exception)")
    time.sleep(sleeptime)
    if random.random() <= exception_probability:
        logging.info(f"raising exception...({sleeptime})")
        raise Exception("Phony exception")
    logging.info(f"returning cleanly ({sleeptime})")
    return sleeptime

def fakefunk(*args, **kwargs):
    return phonyworker(*args, **kwargs)


if __name__ == "__main__":
    fr = concurrent_callers.FunctionRace()
    fr.add_fucntion(phonyworker, args=[1, 10], kwargs={'exception_probability': .7}, count=3)
    fr.add_fucntion(fakefunk, args=[1, 10], kwargs={'exception_probability': .7}, count=3)
    for i in range(2):
        print("============================== New Race! ==============================")
        print("Contestant count after adding them all is:", fr.get_contestant_count())
        print("Contestant spec is:", fr.contestants)
        start_time = time.time()
        print(f"Starting race!")
        try:
            result = fr.start()
        except concurrent_callers.AllFailedException as e:
            print(e)
            result = None
        elapsed = time.time() - start_time
        print("Done, (race duration: {elapsed})result returned by winner is:", result)
        print("awaiting functions to finish...")
        while fr.is_running():
            time.sleep(1)

    print("============================== New Race! ==============================")
    print("final race, exiting immediately after")
    try:
        result = fr.start()
    except concurrent_callers.AllFailedException as e:
        print(e)
        result = None
    print("Done, (race duration: {elapsed})result returned by winner is:", result)
    print("exiting immediately...")

    os._exit(0)
