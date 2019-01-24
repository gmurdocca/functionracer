#!/usr/bin/env python
import concurrrent_callers

def phonyworker(t1, t2, exception_probability=0):
    """
    sleeps for random time between t2 - t1 seconds.
    Raises Exception `exception_probability`*100% of the time
    """
    assert t2>t1
    assert 0 <= exception_probability <= 1

