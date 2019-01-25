#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, ALL_COMPLETED, wait
import threading
import logging
import time
import copy


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
            if done:
                break
            try:
                result = f.result()
                done = True
            except Exception as e:
                exceptions.append(e)
                if running:
                    futures = running
                else:
                    done = True
        if timeout and time.time() - start_time >= timeout:
            exception = TimeoutException()
            done = True
    [f.cancel() for f in futures] # Attempt to cancel any unfinished functions
    try:
        return result
    except NameError:
        exception = AllFailedException(exceptions)
    raise exception


class FunctionRace():

    """
    A Class that represents a race meet for Python functions. Supports adding 
    functions (contestants) and their arguments, running the race, collecting
    results from the winner (ie. the function that returns soonest) and cleanup
    after the race is over (waiting for any unfinished functions to exit).

    Accepts a list of contestant functions and their arguments that will be called 
    in parallel and compete for shortest execution time.

    Each contestant can be any Python function, added via the constructor or via
    `self.add_function`. The list of contestant functions can be edited directly
    before the race starts via `self.contestants`.
    
    Functions must be idempotent or their effect will be multiplied `count` times
    as specified as a parameter in the `add_function` method.
    
    The `start` method will return the result of the first function to exit without
    raising an Exception.  Note that the main Python thread will not exit (eg via 
    sys.exit() or at the end of execution) until all competitors have returned, or
    until os._exit(n) is called. See the following doc regarding os._exit(n):
    https://docs.python.org/2/library/os.html#os._exit
    """

    contestants = {}
    futures = []
    cleaning = False

    def __init__(self, functions=[], timeout=None, cleanup_timeout=5):
        self.timeout = timeout
        self.cleanup_timeout = cleanup_timeout
        self.logger = logging.getLogger()
        for function_spec in functions:
            self.add_function(*function_spec)

    def add_fucntion(self, fn, args=[], kwargs={}, count=1):
        """
        Adds a new contestant function that will compete for shortest runtime.
        `count` number of functions will be added to the start gates in this race.
        """
        self.contestants[fn] = {'args': args, 'kwargs': kwargs, 'count':count}

    def get_contestant_count(self, contestants=None):
        if not contestants:
            contestants = self.contestants
        return sum([contestants[contestant]['count'] for contestant in contestants])

    def _homogenised_contestants(self):
        """
        A generator that yields a homogenous mix of contestants, used for ordering
        them at the start blocks to ensure as fair a start as possible.
        """
        runners = copy.deepcopy(self.contestants)
        while self.get_contestant_count(contestants=runners) > 0:
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
        if self.is_running():
            raise InProgress('Race currently underway.')
        if not self.contestants:
            raise Exception('No Contestants to race! Try adding some...')
        executor = ThreadPoolExecutor(max_workers=self.get_contestant_count())
        for fn, args, kwargs in self._homogenised_contestants():       
            future = executor.submit(fn, *args, **kwargs)
            future.__fname__ = fn.__name__
            future.__fargs__ = args
            future.__fkwargs__ = kwargs
            self.futures.append(future)
        # Gather results as they come in
        exceptions = []
        done = False
        start_time = time.time()
        while not done:
            exited, running = wait(self.futures, timeout=self.timeout, return_when=FIRST_COMPLETED)
            for f in exited:
                if done:
                    break
                try:
                    result = f.result()
                    self.logger.info(f"We have a winner! Name: {f.__fname__}, args: {f.__fargs__}, kwargs: {f.__fkwargs__}")
                    done = True
                except Exception as e:
                    exceptions.append(e)
                    if running:
                        self.futures = list(running)
                    else:
                        done = True
            if self.timeout and time.time() - start_time >= self.timeout:
                exception = TimeoutException()
                done = True
        self.clean(timeout=self.cleanup_timeout)
        try:
            return result
        except NameError:
            exception = AllFailedException(exceptions)
        raise exception

    def is_running(self):
        return self.futures and True or False

    def clean(self, timeout=None):
        """
        Background worker thread that Waits `timeout` seconds (or indefinitely if None)
        for any non-winners to return.

        Note that the main Python thread will not exit until this thread returns, ie
        until all competitors have returned. You can forcibly exit main thread by calling
        os._exit(n). See the following doc regarding os._exit(n):
        https://docs.python.org/2/library/os.html#os._exit

        Raises CleanupFailed exception if `timeout` seconds is exceeded.
        Raises InProgress exception if a cleanup is currently underway.
        """
        self.logger.debug("Starting Cleanup...")
        if self.cleaning:
            raise InProgress('Cleanup currently underway.')
        def cleanall(self, timeout):
            while self.is_running():
                exited, running = wait(self.futures, timeout=timeout, return_when=ALL_COMPLETED)
                self.futures = list(running)
            self.cleaning = False
        self.cleaning = True
        cleanall_thread = threading.Thread(target=cleanall, args=(self, timeout))
        cleanall_thread.start()

