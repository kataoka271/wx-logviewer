import time
from mmap import ACCESS_READ, mmap
from multiprocessing import Condition, Process
from multiprocessing.shared_memory import SharedMemory
from multiprocessing.sharedctypes import RawValue, Value
from typing import Any
from zlib import decompress

from blfparser import (BaseObject, CANMessage, EthernetFrame,
                       parse_can_fd_message, parse_can_fd_message_64,
                       parse_can_message, parse_ethernet_frame,
                       parse_ethernet_frame_ex, parse_file_header)
from constants import (CAN_FD_MESSAGE, CAN_FD_MESSAGE_64, CAN_MESSAGE,
                       CAN_MESSAGE2, ETHERNET_FRAME, ETHERNET_FRAME_EX, LOBJ,
                       LOG_CONTAINER, LOG_CONTAINER_STRUCT, NO_COMPRESSION,
                       OBJ_HEADER_BASE_STRUCT, OBJ_HEADER_V1_STRUCT,
                       OBJ_HEADER_V2_STRUCT, TIME_TEN_MICS, ZLIB_DEFLATE)


class QueueBuf:
    def __init__(self, size) -> None:
        self.shm = SharedMemory(create=True, size=size)
        self.q_idx_met = Condition()
        self.q_idx = RawValue("I", 0)
        self.room_available = Condition()
        self.item_available = Condition()
        self.q_top = RawValue("I", 0)
        self.q_end = RawValue("I", 0)
        self.q_close = RawValue("B", 0)

    def write(self, idx, data):
        n = self.shm.size
        k = len(data)
        if k > n:
            raise Exception(f"shared memory is too short to store the data: {k} > {n}")
        with self.q_idx_met:
            with self.room_available:
                while self.q_idx.value != idx:
                    self.q_idx_met.wait()
                self.q_idx.value += 1
                while True:
                    q = self.q_end.value
                    p = self.q_top.value
                    q_ = q + k
                    if p <= q and q_ < n:
                        self.q_end.value = q_
                        break
                    if p <= q and q_ >= n and q_ - n < p:
                        self.q_end.value = q_ = q_ - n
                        break
                    if p > q and q_ < p:
                        self.q_end.value = q_
                        break
                    self.room_available.wait()
                self.q_idx_met.notify_all()
        if q < q_:
            self.shm.buf[q:q_] = data
        else:
            r = n - q
            self.shm.buf[q:] = data[:r]
            self.shm.buf[:q_] = data[r:]
        with self.item_available:
            self.item_available.notify_all()

    def read(self, size):
        n = self.shm.size
        with self.item_available:
            while True:
                p = self.q_top.value
                q = self.q_end.value
                p_ = p + size
                if p < q and p_ <= q:
                    self.q_top.value = p_
                    break
                if p > q and p_ < n:
                    self.q_top.value = p_
                    break
                if p > q and p_ >= n and p_ - n <= q:
                    self.q_top.value = p_ = p_ - n
                    break
                if p == q and self.q_close.value:
                    return None
                self.item_available.wait()
        with self.room_available:
            self.room_available.notify_all()
        if p < p_:
            return self.shm.buf[p:p_]
        else:
            return memoryview(self.shm.buf[p:].tobytes() + self.shm.buf[:p_].tobytes())

    def close(self):
        self.q_close.value = 1


def parse_log_container_sync(fp, idx, pos):
    while True:
        with idx:
            fp.seek(pos.value)
            data = fp.read(OBJ_HEADER_BASE_STRUCT.size)
            if not data:
                return  # successfully EOF
            if len(data) < OBJ_HEADER_BASE_STRUCT.size:
                raise Exception("truncated base object header")
            header = OBJ_HEADER_BASE_STRUCT.unpack(data)
            if header[0] != LOBJ:
                raise Exception("no magic number LOBJ (log container)")
            # header_size = header[1]
            # version = header[2]
            obj_size = header[3]
            obj_type = header[4]
            pos.value += obj_size + obj_size % 4
            i = idx.value
            idx.value += 1
        if obj_type != LOG_CONTAINER:
            raise Exception("obj_type not equal to LOG_CONTAINER")
        data = fp.read(LOG_CONTAINER_STRUCT.size)
        if len(data) < LOG_CONTAINER_STRUCT.size:
            raise Exception("truncated log container header")
        compression_method, uncompressed_size = LOG_CONTAINER_STRUCT.unpack(data)
        data_size = obj_size - OBJ_HEADER_BASE_STRUCT.size - LOG_CONTAINER_STRUCT.size
        data = fp.read(data_size)
        read_size = len(data)
        if read_size < data_size:
            raise Exception("truncated log container body")
        if compression_method == NO_COMPRESSION:
            pass
        elif compression_method == ZLIB_DEFLATE:
            data = decompress(data, 15, uncompressed_size)
        else:
            raise Exception("unknown compression method")
        yield (i, data)


def parse_base_object_sync(q: QueueBuf, idx, object_count, start_timestamp, stop_timestamp):
    while True:
        data = q.read(OBJ_HEADER_BASE_STRUCT.size)
        if data is None:
            break
        header = OBJ_HEADER_BASE_STRUCT.unpack(data)
        if header[0] != LOBJ:
            raise Exception("no magic number LOBJ (base object)")
        # header_size = header[1]
        version = header[2]
        obj_size = header[3]
        obj_type = header[4]
        if obj_type in (CAN_FD_MESSAGE_64, ETHERNET_FRAME_EX):
            size = obj_size - OBJ_HEADER_BASE_STRUCT.size
        else:
            size = obj_size - OBJ_HEADER_BASE_STRUCT.size + obj_size % 4
        data = q.read(size)
        if data is None:
            break
        continue
        if version == 1:
            m = OBJ_HEADER_V1_STRUCT.unpack_from(data)
            flags = m[0]
            timestamp = m[3]
            i = OBJ_HEADER_V1_STRUCT.size
        elif version == 2:
            m = OBJ_HEADER_V2_STRUCT.unpack_from(data)
            flags = m[0]
            timestamp = m[3]
            i = OBJ_HEADER_V2_STRUCT.size
        else:
            raise Exception("unknown header version")
        obj_data = data[i:obj_size - OBJ_HEADER_BASE_STRUCT.size]

        if flags == TIME_TEN_MICS:
            time_ns = timestamp * 10000
        else:
            time_ns = timestamp
        msg: CANMessage | EthernetFrame | None
        if obj_type in (CAN_MESSAGE, CAN_MESSAGE2):
            msg = parse_can_message(obj_data)
        elif obj_type == CAN_FD_MESSAGE:
            msg = parse_can_fd_message(obj_data)
        elif obj_type == CAN_FD_MESSAGE_64:
            msg = parse_can_fd_message_64(obj_data)
        elif obj_type == ETHERNET_FRAME:
            msg = parse_ethernet_frame(obj_data)
        elif obj_type == ETHERNET_FRAME_EX:
            msg = parse_ethernet_frame_ex(obj_data)
        else:
            msg = None
        item: BaseObject = {"type": "base",
                            "object_count": object_count,
                            "start_timestamp": start_timestamp,
                            "stop_timestamp": stop_timestamp,
                            "time_ns": time_ns,
                            "obj_type": obj_type,
                            "obj_data": obj_data,
                            "msg": msg}
        yield item


def source(q: QueueBuf, filename, idx, pos):
    with open(filename, "rb") as fp:
        with mmap(fp.fileno(), length=0, access=ACCESS_READ) as mm:
            it = parse_log_container_sync(fp, idx, pos)
            for i, data in it:
                q.write(i, data)


def consume(q: QueueBuf, idx, object_count, start_timestamp, stop_timestamp):
    for item in parse_base_object_sync(q, idx, object_count, start_timestamp, stop_timestamp):
        pass


def main():
    # filename = sys.argv[1]
    filenames = [r"C:\Users\k_hir\Projects\numba-test\test.blf"]
    t1 = time.time()
    for filename in filenames:
        with open(filename, "rb") as fp:
            object_count, start_timestamp, stop_timestamp = parse_file_header(fp)
            offset = fp.tell()
        q = QueueBuf(size=10_000_000)
        idx0 = Value("I", 0)
        pos0 = Value("I", offset)
        ps = [Process(target=source, args=(q, filename, idx0, pos0)) for _ in range(1)]
        for p0 in ps:
            p0.start()
        idx1 = Value("I", 0)
        cs = [Process(target=consume, args=(q, idx1, object_count, start_timestamp, stop_timestamp)) for _ in range(1)]
        for c0 in cs:
            c0.start()
        for p0 in ps:
            p0.join()
        q.close()
        for c0 in cs:
            c0.join()
    t2 = time.time()
    print(t2 - t1)


if __name__ == "__main__":
    main()
