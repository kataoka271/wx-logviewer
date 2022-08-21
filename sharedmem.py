from multiprocessing import Condition, Event, Process
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.sharedctypes import RawValue
import time
import random
import struct


def produce(shm: SharedMemory, r_ptr, w_ptr, w_close):
    size = shm.size
    buf = shm.buf
    left_size = 2000
    while left_size > 0:
        data_size = 4
        w_ptr_ = w_ptr.value + data_size
        if w_ptr_ > size:
            while r_ptr.value == 0:
                time.sleep(0.5)  # queue is full
            w_ptr.value = 0
            w_ptr_ = data_size
        while w_ptr.value < r_ptr.value <= w_ptr_:
            time.sleep(0.5)  # queue is full
        time.sleep(random.random() / 4 + 0.1)
        buf[w_ptr.value:w_ptr_] = struct.pack(">I", left_size)
        w_ptr.value = w_ptr_
        print("write", w_ptr.value)
        left_size -= data_size
    print("finish")
    w_close.value = 1


def consume(shm: SharedMemory, r_ptr, w_ptr, w_close):
    size = shm.size
    buf = shm.buf
    while not (w_close.value == 1 and w_ptr.value == r_ptr.value):
        while w_ptr.value == r_ptr.value:
            time.sleep(0.5)  # queue is empty
        data_size = 4
        r_ptr_ = r_ptr.value + data_size
        if r_ptr_ > size:
            r_ptr.value = 0
            r_ptr_ = data_size
        data = buf[r_ptr.value:r_ptr_]
        r_ptr.value = r_ptr_
        print("read", struct.unpack(">I", data.tobytes()))
        time.sleep(random.random() + 0.1)


def main():
    r_ptr = RawValue("I", 0)
    w_ptr = RawValue("I", 0)
    w_close = RawValue("I", 0)
    shm = SharedMemory(size=1000, create=True)
    p1 = Process(target=produce, args=(shm, r_ptr, w_ptr, w_close))
    p2 = Process(target=consume, args=(shm, r_ptr, w_ptr, w_close))
    p1.start()
    p2.start()
    p1.join()
    p2.join()
    p1.close()
    p2.close()


if __name__ == "__main__":
    main()
