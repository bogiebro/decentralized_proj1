from contextlib import contextmanager
from threading  import Lock
from collections import defaultdict

# NOTE: currently disabled to test concurrency
class RWLock:
    def __init__(self):
      self.w_lock = Lock()
      self.num_r_lock = Lock()
      self.num_r = 0

    def r_acquire(self):
      return
      with self.num_r_lock:
        self.num_r += 1
        if self.num_r == 1:
          self.w_lock.acquire()

    def r_release(self):
      return 
      assert self.num_r > 0
      with self.num_r_lock:
        self.num_r -= 1
        if self.num_r == 0:
          self.w_lock.release()

    def w_acquire(self):
      return
      self.w_lock.acquire()

    def w_release(self):
      return
      self.w_lock.release()

    @contextmanager
    def r_locked(self):
      try:
        self.r_acquire()
        yield
      finally:
        self.r_release()

    @contextmanager
    def w_locked(self):
      try:
        self.w_acquire()
        yield
      finally:
        self.w_release()

class RWLockDict:
  def __init__(self):
    self.locks = defaultdict(RWLock)
    self.global_lock = Lock()
    self.num_w = 0
    self.num_w_lock = Lock()

  @contextmanager
  def r_locked(self, key):
    lock = self.locks[key]
    try:
      lock.r_acquire()
      yield
    finally:
      lock.r_release()
      if lock.w_lock.acquire(blocking=False):
        del self.locks[key]

  @contextmanager
  def all_locked(self):
    try:
      self.global_lock.acquire()
      yield
    finally:
      self.global_lock.release()

  @contextmanager
  def w_locked(self, key):
    lock = self.locks[key]
    try:
      with self.num_w_lock:
        self.num_w += 1
        if self.num_w == 1:
          self.global_lock.acquire()
      lock.w_acquire()
      yield
    finally:
      lock.w_release()
      if lock.w_lock.acquire(blocking=False):
        del self.locks[key]
      assert self.num_w > 0
      with self.num_w_lock:
        self.num_w -= 1
        if self.num_w == 0:
          self.global_lock.release()
