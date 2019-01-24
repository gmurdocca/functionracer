#!/usr/bin/env python
import concurrent_callers
import logging
import random
import time
import sys

logging.getLogger().setLevel(logging.NOTSET)

def phonyworker(t1, t2, exception_probability=0):
    """
    sleeps for random time between t2 - t1 seconds.
    Raises Exception `exception_probability`*100% of the time
    """
    assert t2 >= t1
    assert 0 <= exception_probability <= 1

    sleeptime = random.randint(t1, t2)
    logging.info(f"Sleeping {sleeptime}s ({exception_probability*100}% chance of Exception)")
    time.sleep(sleeptime)
    if random.random() <= exception_probability:
        logging.info("raising exception...")
        raise Exception("Phony exception")
    logging.info("returning cleanly")
    return sleeptime

def fakefunk(*args, **kwargs):
    return phonyworker(*args, **kwargs)


if __name__ == "__main__":
    result = concurrent_callers.concurrent_call(phonyworker, count=100, timeout=None, args=[1,100], kwargs={'exception_probability': .5})
    print(result)
    print("calling sys.exit()")
    sys.exit()

    fr = concurrent_callers.FunctionRace(do_cleanup=True)
    print(fr.do_cleanup)
    fr.add_fucntion(phonyworker, args=[1, 5], kwargs={'exception_probability': .33}, count=4)
    print(fr.get_contestant_count())
    fr.add_fucntion(fakefunk, args=[1, 5], kwargs={'exception_probability': .33}, count=4)
    print(fr.get_contestant_count())
    print(fr.contestants)
    result = fr.start()
    print("Done, result returned by winner is:", result)
    

