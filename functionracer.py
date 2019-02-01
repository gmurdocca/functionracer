#!/usr/bin/env python
from concurrent.futures import ThreadPoolExecutor, FIRST_COMPLETED, ALL_COMPLETED, wait
from concurrent.futures.thread import _python_exit
import threading
import logging
import atexit
import time
import copy


class TimeoutException(Exception):
    pass

class AllFailedException(Exception):
    pass

class InProgress(Exception):
    pass

class CleanupFailed(Exception):
    pass

class FunctionRacer():

    """
    A Class that represents a race meet for Python functions. Supports adding 
    functions (contestants) and their arguments, running the race, collecting
    results from the winner (ie. the function that returns soonest) and optional
    cleanup after the race is over (waiting for any unfinished functions to exit).

    Accepts a list of contestant functions and their arguments that will be called 
    in parallel and compete for shortest execution time.

    Each contestant can be any Python function, added via the constructor or via
    `self.add_function`. The list of contestant functions can be edited directly
    before the race starts via `self.contestants`.
    
    Functions must be idempotent or their effect will be multiplied `count` times
    as specified as a parameter in the `add_function` method.
    
    The `start` method will return the result of the first function to exit without
    raising an Exception. Once the `start` method returns, cleanup begins.

    The `results` attribute contains a list of concurrent.futures.Future objects,
    each representing a competitor, in order of race position as they finish. Each
    future will have the following extra attributes to those documented:

            future.start_time  - the start time in secs since epoch of the function
            future.end_time    - the end time in secs since epoch of the function
            future.duration    - the duration of the function in seconds
            future.__fname__   - the name of the python function (race competitor)
            future.__fargs__   - a list of args passed to the function
            future.__fkwargs__ - a dict of keyword args passed to the function

    See here for documentation about concurrent.futures.Future objects:
    https://docs.python.org/3/library/concurrent.futures.html#future-objects

    If no function call finishes within `timeout` seconds, TimeoutException will
    be raised, else wait indefinitely if `timeout` is None (default).
    
    If cleanup_wait=True, the main Python thread will not exit (eg. via sys.exit()
    or at the end of execution) until all competitors have returned, or until
    os._exit(n) is called. If `cleanup_timeout` seconds elapse during cleanup, an
    CleanupFailed exception is raised, however the main Python thread will continue
    running until all compeditors exit.
    
    If cleanup_wait=False or if os._exit(n) is used, the main Python thread will
    exit even if compeditors have not yet completed running. Note this may have
    dangerous effects as unfinished (non-winning) compeditor functions will be
    killed mid-execution.

    Note: Because cleanup_wait applies to the entire Python interpreter, it is set
    globally. If you have multiple instances of the FunctionRacer class, the state
    of cleanup_wait for all instances will be set to what ever it was last set to.
    It can be set via the constructor, or via the `set_cleanp_wait` method.
    
    See the following doc regarding os._exit(n):
    https://docs.python.org/2/library/os.html#os._exit
    """

    contestants = {}
    futures = []
    results = []
    cleaning = False
    cleanup_wait = True

    def __init__(self, functions=[], timeout=None, cleanup_wait=True, cleanup_timeout=5):
        self.timeout = timeout
        self.cleanup_timeout = cleanup_timeout
        self.set_cleanup_wait(cleanup_wait)
        self.logger = logging.getLogger()
        for function_spec in functions:
            self.add_function(*function_spec)

    def set_cleanup_wait(self, desired_state):
        cls = self.__class__
        assert(type(desired_state) == bool)
        current_state = cls.cleanup_wait
        if not current_state == desired_state:
            if desired_state == True:
                atexit.register(_python_exit)
            else:
                atexit.unregister(_python_exit)
            cls.cleanup_wait = desired_state

    def add_function(self, fn, args=[], kwargs={}, count=1):
        """
        Adds a new contestant function that will compete for shortest runtime.
        `count` number of functions will be added to the start gates in this race.
        """
        self.contestants[fn] = {'args': args, 'kwargs': kwargs, 'count': count}

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

    def _done_callback(self, future):
        """
        Called by each done future as they return or raise.
        """
        future.end_time = time.time()
        future.duration = future.end_time - future.start_time
        # sort the results list in order of fastest to slowest
        self.results.sort(key=lambda e: - 1/(e.duration or -1))

    def start(self):
        """
        Starts the race.    

        Waits for the first function that returns without raising an exception and
        returns its result.
        
        If all function calls raise an Exception, AllFailedException will be raised.

        If no function call finishes within `self.timeout` seconds, return TimeoutException,
        else wait indefinitely.

        Raises InProgress exception if a race is already under way.
        """
        if self.is_running():
            raise InProgress('Race currently underway.')
        if not self.contestants:
            raise Exception('No Contestants to race! Try adding some...')
        executor = ThreadPoolExecutor(max_workers=self.get_contestant_count())
        for fn, args, kwargs in self._homogenised_contestants():       
            future = executor.submit(fn, *args, **kwargs)
            future.start_time = time.time()
            future.end_time = None
            future.duration = None
            future.add_done_callback(self._done_callback)
            future.__fname__ = fn.__name__
            future.__fargs__ = args
            future.__fkwargs__ = kwargs
            self.futures.append(future)
        self.results = self.futures[:]
        # Gather results as they come in
        exception = None
        done = False
        start_time = time.time()
        while not done:
            exited, running = wait(self.futures, timeout=self.timeout, return_when=FIRST_COMPLETED)
            for f in exited:
                if done:
                    break
                try:
                    result = f.result()
                    self.logger.debug(f"We have a winner! Name: {f.__fname__}, args: {f.__fargs__}, kwargs: {f.__fkwargs__}")
                    done = True
                except Exception as e:
                    if running:
                        self.futures = list(running)
                    else:
                        done = True
            self.logger.debug(f"self.timeout={self.timeout}, start_time={start_time}, current_time={time.time()} delta = {time.time() - start_time}")
            if self.timeout and time.time() - start_time >= self.timeout:
                exception = TimeoutException(f"No functions returned within {self.timeout} seconds.")
                done = True
        self.clean(cleanup_wait=self.cleanup_wait, cleanup_timeout=self.cleanup_timeout)
        try:
            return result
        except NameError:
            if not exception:
                exception = AllFailedException("All contestant functions returned an Exception")
        raise exception

    def is_running(self):
        return self.futures and True or False

    def clean(self, cleanup_wait=True, cleanup_timeout=None):
        """
        Spawns a background worker thread that Waits `timeout` seconds (or indefinitely if None)
        for any non-winners to return.

        If cleanup_wait=True, the main Python thread will not exit until this thread returns, ie
        until all competitors have returned. You can forcibly exit main thread by calling
        os._exit(n). See the following doc regarding os._exit(n):
        https://docs.python.org/2/library/os.html#os._exit
        
        Note that setting cleanup_wait=False or using os._exit() is dangerous as unfinished (non-winning)
        functions will be killed mid-execution.

        Raises CleanupFailed exception if `timeout` seconds is exceeded.
        Raises InProgress exception if a cleanup is currently underway.
        """
        self.logger.debug(f"Starting clean(cleanup_wait={cleanup_wait}, cleanup_timeout={cleanup_timeout})...")
        if self.cleaning:
            raise InProgress('Cleanup currently underway.')
        def cleanall(self, cleanup_wait, cleanup_timeout):
            self.logger.debug(f"Starting cleanall(cleanup_wait={cleanup_wait}, cleanup_timeout={cleanup_timeout})...")
            self.cleaning = True
            if cleanup_wait:
                while self.is_running():
                    exited, running = wait(self.futures, timeout=cleanup_timeout, return_when=ALL_COMPLETED)
                    self.futures = list(running)
                    if running:
                        final_cleanall_thread = threading.Thread(target=cleanall, args=(self, cleanup_wait, None))
                        final_cleanall_thread.start()
                        raise CleanupFailed(f"{len(self.futures)} functions failed to return within timeout={cleanup_timeout} seconds")
            else:
                # we deregistered the atexit callback call to thread.join(), just reinit our futures list.
                self.futures = []
            self.cleaning = False
        cleanall_thread = threading.Thread(target=cleanall, args=(self, cleanup_wait, cleanup_timeout))
        cleanall_thread.start()

