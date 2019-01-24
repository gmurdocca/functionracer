#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, ALL_COMPLETED, wait
import threading
import logging
import time


class TimeoutException(Exception):
    pass

class AllFailedException(Exception):
    pass

class InProgress(Exception):
    pass

def concurrent_call(fn, count=1, timeout=None, args=[], kwargs={}):
    """
    Calls `count` concurrent instances of function `fn`, passing args and kwargs as follows:
    
        fn(*args, **kwargs)

    Note: Assumes `fn` is idempotent.

    Waits for the first call that returns without raising an exception and returns its result.
    If all calls raise an Exception, returns AllFailedException containing a list of all the Exceptions.
    If all calls do not finish within `timeout` seconds, return TimeoutException, else wait indefinitely.
    """
    executor = ThreadPoolExecutor(max_workers=count)
    futures = []
    for i in range(count):       
        future = executor.submit(fn, *args, **kwargs)
        futures.append(future)
    exceptions = []
    done = False
    start_time = time.time()
    while not done:
        exited, running = wait(futures, timeout=timeout, return_when=FIRST_COMPLETED)
        for f in exited:
            try:
                result = f.result()
                done = True
            except Exception as e:
                exceptions.append(e)
                if running:
                    futures = running
                else:
                    done = True
        if time.time() - start_time >= timeout:
            result = TimeoutException()
            done = True
    [f.cancel() for f in futures] # Attempt to cancel any unfinished functions
    try:
        return result
    except NameError:
        return AllFailedException(exceptions)


class FunctionRace():

    """
    A Class that represents a race meet for Python functions. Supports adding 
    functions (contestants) and their arguments, running the race, collecting
    results from the winner (ie. the function that returns soonest) and cleanup
    after the race is over (killing any unfinished functions).

    Accepts a list of contestant functions and their arguments that will be called 
    in parallel and compete for shortest execution time.

    Each contestant can be any Python function, added via the constructor or via
    `self.add_function`. Any mix of the same function or different functions can
    be added to the race. The list of contestant functions can be edited directly
    before the race starts via self.contestants
    
    Functions must be idempotent or their effect will be multiplied `count` times
    as specified in `add_function`. 
    """

    contestants = {}
    timeout = None
    futures = []
    cleaning = False
    do_cleanup=True

    def __init__(self, functions=[], timeout=None, do_cleanup=True):
        self.timeout = timeout
        self.do_cleanup = do_cleanup
        self.logger = logging.getLogger()
        for function_spec in functions:
            self.add_function(*function_spec)

    def add_fucntion(self, fn, args=[], kwargs={}, count=1):
        """
        Adds a new contestant function that will compete for shortest runtime.
        `count` number of functions will be added to the start gates in this race.
        """
        self.contestants[fn] = {'args': args, 'kwargs': kwargs, 'count':count}

    def get_contestant_count(self, contestants=self.contestants):
        return sum([contestants[contestant]['count'] for contestant in contestants])

    def _homogenised_contestants(self):
        """
        A generator that yields a homogenous mix of contestants, used for ordering
        them at the start blocks to ensure as fair a start as possible.
        """
        runners = self.contestants.copy()
        while get_contestant_count(contestants=runners) > 0:
            for runner in runners:
                if runners[runner]['count']:
                    runners[runner]['count'] -= 1
                    yield runner, runners[runner]['args'], runners[runner]['kwargs']

    def start(self):
        """
        Starts the race.    

        Waits for the first function that returns without raising an exception and
        returns its result.
        
        If all function calls raise an Exception, returns AllFailedException containing
        a list of all the Exceptions.

        If all function calls do not finish within `self.timeout` seconds, return
        TimeoutException, else wait indefinitely.

        Raises InProgress exception if a race is already under way.
        """
        if self.futures:
            raise InProgress('Race currently underway.')
        executor = ThreadPoolExecutor(max_workers=get_contestant_count())
        for fn, args, kwargs in self._homogenised_contestants():       
            future = executor.submit(fn, *args, **kwargs)
            future.__fname__ = fn.__name__
            future.__fargs__ = args
            future.__fkwargs = kwargs
            self.futures.append(future)
        # Gather results as they come in
        exceptions = []
        done = False
        start_time = time.time()
        while not done:
            exited, running = wait(self.futures, timeout=self.timeout, return_when=FIRST_COMPLETED)
            for f in exited:
                try:
                    result = f.result()
                    logger.info(f"We have a winner! Name: {f.__fname__}, args: {f.__fargs__}, kwargs: {f.__fkwargs__}")
                    done = True
                except Exception as e:
                    exceptions.append(e)
                    if running:
                        self.futures = running
                    else:
                        done = True
            if time.time() - start_time >= self.timeout:
                result = TimeoutException()
                done = True
        if self.do_cleanup:
            self.clean()
        try:
            return result
        except NameError:
            return AllFailedException(exceptions)

    def is_running(self):
        return self.futures and True or False

    def clean(self, background=True, timeout=None):
        """
        Attempt to cancel any unfinished functions, effectively resetting the race.
        If background=True, return before cleanup is completed, else block until then.
        Completion of backgrounded cleanup operation can be checked by polling self.is_running().
        Wait `timeout` seconds if provided before returning, else block indefinitely.
        If there are still running contestents after timeout expiry, a list of them will
        be returned.
        Raises InProgress Exception if a cleanup is currently underway.
        """
        if self.cleaning:
            raise InProgress('Cleanup currently underway.')
        def killall(self, timeout):
            [f.cancel() for f in self.futures]
            while self.is_running():
                exited, running = wait(self.futures, timeout=timeout, return_when=ALL_COMPLETED)
                self.futures = running
            self.cleaning = False
            return self.futures or None

        self.cleaning = True
        if background:
            killall_thread = threading.Thread(target=killall, args=(self, timeout))
            killall_thread.start()
        else:
            return killall(self, timeout)


