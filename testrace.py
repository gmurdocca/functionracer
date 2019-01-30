#!/usr/bin/env python
import functionracer
import logging
import random
import time
import sys
import os

logging.getLogger().setLevel(logging.NOTSET)


##############################
## Some fake worker functions
##############################

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
        logging.info(f"I am a contestant, pretending something went wrong and raising an Exception after {sleeptime} seconds.")
        raise Exception("Phony exception")
    return f"I'm a contestant and I pretended to do {sleeptime} seconds worth of work ;-)."

def fakefunk(*args, **kwargs):
    return phonyworker(*args, **kwargs)


##########################################
## main: Configure and start a few races
##########################################

if __name__ == "__main__":
    #f = functionracer.FunctionRacer(functions=[(phonyworker, [1, 10], {'exception_probability': .7}, 3), ])
    fr = functionracer.FunctionRacer(timeout=1, cleanup_wait=True, cleanup_timeout=2)
    fr.add_function(phonyworker, args=[1, 10], kwargs={'exception_probability': .7}, count=3)
    fr.add_function(fakefunk, args=[1, 10], kwargs={'exception_probability': .7}, count=3)
    repeat_race = 3 # repeat the race 3 times
    for i in range(repeat_race):
        print("============================== New Race! ==============================")
        print("Contestant count after adding them all is:", fr.get_contestant_count())
        start_time = time.time()
        print(f"Starting race!")
        try:
            result = fr.start()
            print(f"Winner ran in {time.time() - start_time} seconds. Result returned was: {result}")
        except functionracer.AllFailedException as e:
            print(f"Sorry no winners (race duration: {time.time() - start_time} seconds), all contestants raised these exceptions:", e)
        except functionracer.TimeoutException as e:
            print(f"Sorry no winners (race duration: {time.time() - start_time} seconds):", e)

        if i == repeat_race - 1:
            # This is the last race
            print("That was the last race, forcibly exiting (not waiting for non-winners to return) by calling os._exit(0)")
            os._exit(0)
        else:
            print("Awaiting contestant functions to finish (unless we set cleanup_wait=False)...")
            while fr.is_running():
                time.sleep(1)
            print("All contestant functions are returned (or we skipped the check with cleanup_wait=False), the race is over.")

