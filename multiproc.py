import os
import random
import time
from multiprocessing import Pool
from multiprocessing.pool import AsyncResult


def long_time_task(name, buff):
    print('Run task {} ({})...'.format(name, os.getpid()))
    start = time.time()
    v = int(random.random() * 255)
    for i in range(len(buff)):
        buff[i] = v
    end = time.time()
    ret = sum(buff)
    print('Task {} runs {} seconds.'.format(name, (end - start)))
    return ret


def dispatcher():
    start = time.time()
    buff = bytearray(100_000_000)
    print('Parent process {}.'.format(os.getpid()))
    p = Pool()
    rets: list[AsyncResult] = []
    for i in range(20):
        rets.append(p.apply_async(long_time_task, args=(i, buff)))
    print('Waiting for all subprocesses done...')
    for ret in rets:
        a = ret.get()
        print(a)
    p.close()
    p.join()
    end = time.time()
    print('All subprocesses done. {} seconds.'.format(end - start))


if __name__ == "__main__":
    dispatcher()
