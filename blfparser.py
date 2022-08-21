from multiprocessing import Lock
import time
from datetime import datetime
from mmap import ACCESS_READ, mmap
from typing import BinaryIO, Iterator, Literal, TypedDict
from zlib import decompress

from constants import (BRS, BRS_64, CAN_ERROR, CAN_ERROR_EXT, CAN_FD_MESSAGE,
                       CAN_FD_MESSAGE_64, CAN_FD_MESSAGE_64_STRUCT,
                       CAN_FD_MESSAGE_STRUCT, CAN_MESSAGE, CAN_MESSAGE2,
                       CAN_MESSAGE_STRUCT, CAN_MSG_EXT, DIR, DIR_64, DIR_64_S,
                       DLC_MAP, ESI, ESI_64, ETHERNET_FRAME, ETHERNET_FRAME_EX,
                       ETHERNET_FRAME_EX_STRUCT, ETHERNET_FRAME_STRUCT, FDF,
                       FDF_64, FILE_HEADER_STRUCT, FORWARDED, GLOBAL_MARKER,
                       LOBJ, LOG_CONTAINER, LOG_CONTAINER_STRUCT, LOGG,
                       NO_COMPRESSION, OBJ_HEADER_BASE_STRUCT,
                       OBJ_HEADER_V1_STRUCT, OBJ_HEADER_V2_STRUCT, RTR, RTR_64,
                       TIME_ONE_NANS, TIME_TEN_MICS, VALID_CHECKSUM,
                       VALID_FRAME_HANDLE, VALID_HW_CHANNEL,
                       VLAN_TPID_TCI_TYPE, ZLIB_DEFLATE)


class CANMessage(TypedDict):
    type: Literal["can"]
    channel: int
    dir: int
    can_id: int
    dlc: int
    rtr: bool
    fdf: bool
    brs: bool
    esi: bool
    data: memoryview


class EthernetFrame(TypedDict):
    type: Literal["ethernet"]
    channel: int
    hw_channel: int
    dir: int
    mac_da: bytes
    mac_sa: bytes
    vlan_tpid: int
    vlan_pri: int
    vlan_id: int
    eth_type: int
    data: memoryview


class BaseObject(TypedDict):
    type: Literal["base"]
    object_count: int
    start_timestamp: int
    stop_timestamp: int
    time_ns: int
    obj_type: int
    obj_data: memoryview
    msg: CANMessage | EthernetFrame | None


def to_nanosecond(year: int, month: int, weekday: int, day: int, hour: int, minute: int, second: int, millisecond: int) -> int:
    try:
        t = datetime(year, month, day, hour, minute, second, millisecond * 1000)
    except ValueError:
        return 0
    else:
        return int(round(t.timestamp() * 1e9))


def parse_file_header(fp: mmap | BinaryIO) -> tuple[int, int, int]:
    data = fp.read(FILE_HEADER_STRUCT.size)
    header = FILE_HEADER_STRUCT.unpack(data)
    if header[0] != LOGG:
        raise Exception("no magic number LOGG")
    header_size = header[1]
    # file_size = header[10]
    # uncompressed_size = header[11]
    object_count = header[12]
    start_timestamp = to_nanosecond(*header[14:22])
    stop_timestamp = to_nanosecond(*header[22:30])
    fp.read(header_size - FILE_HEADER_STRUCT.size)
    return (object_count, start_timestamp, stop_timestamp)


def parse_log_container(fp: mmap | BinaryIO) -> Iterator[bytes]:
    while True:
        data = fp.read(OBJ_HEADER_BASE_STRUCT.size)
        if not data:
            return  # successfully EOF
        if len(data) < OBJ_HEADER_BASE_STRUCT.size:
            raise Exception("truncated base object header")
        header = OBJ_HEADER_BASE_STRUCT.unpack(data)
        if header[0] != LOBJ:
            raise Exception("no magic number LOBJ")
        # header_size = header[1]
        # version = header[2]
        obj_size = header[3]
        obj_type = header[4]
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
        yield data
        fp.read(obj_size % 4)


def parse_log_container_mm(mm: mmap) -> Iterator[bytes]:
    pos = mm.tell()
    end = mm.size()
    while pos < end:
        header = OBJ_HEADER_BASE_STRUCT.unpack_from(mm, pos)
        if header[0] != LOBJ:
            raise Exception("no magic number LOBJ")
        # header_size = header[1]
        # version = header[2]
        obj_size = header[3]
        obj_type = header[4]
        if obj_type != LOG_CONTAINER:
            raise Exception("obj_type not equal to LOG_CONTAINER")
        compression_method, uncompressed_size = LOG_CONTAINER_STRUCT.unpack_from(mm, pos + OBJ_HEADER_BASE_STRUCT.size)
        data = mm[pos + OBJ_HEADER_BASE_STRUCT.size + LOG_CONTAINER_STRUCT.size:pos + obj_size]
        if compression_method == NO_COMPRESSION:
            pass
        elif compression_method == ZLIB_DEFLATE:
            data = decompress(data, 15, uncompressed_size)
        else:
            raise Exception("unknown compression method")
        yield data
        pos = pos + obj_size + obj_size % 4


def q_add(buf: memoryview, first: int, last: int, data: bytes) -> None | tuple[int, int]:
    n = len(buf)
    p = last + len(data)
    r = p - n
    if r > 0:
        if r > first:
            return None  # queue is full
        else:
            buf[last:] = data[:-r]
            buf[:r] = data[-r:]
            last = r
    else:
        buf[last:p] = data
        last = p
    return (first, last)


def parse_base_object(buf: memoryview, first: int, last: int, object_count: int, start_timestamp: int, stop_timestamp: int) -> Iterator[BaseObject]:
    while first < last:
        if last - first < OBJ_HEADER_BASE_STRUCT.size:
            break  # need more data
        header = OBJ_HEADER_BASE_STRUCT.unpack_from(buf, first)
        if header[0] != LOBJ:
            raise Exception("no magic number LOBJ")
        # header_size = header[1]
        version = header[2]
        obj_size = header[3]
        obj_type = header[4]
        if last - first < obj_size:
            break  # need more data
        i = first + OBJ_HEADER_BASE_STRUCT.size
        if version == 1:
            m = OBJ_HEADER_V1_STRUCT.unpack_from(buf, i)
            flags = m[0]
            timestamp = m[3]
            i += OBJ_HEADER_V1_STRUCT.size
        elif version == 2:
            m = OBJ_HEADER_V2_STRUCT.unpack_from(buf, i)
            flags = m[0]
            timestamp = m[3]
            i += OBJ_HEADER_V2_STRUCT.size
        else:
            raise Exception("unknown header version")
        obj_data = buf[i:first + obj_size]
        if obj_type not in (CAN_FD_MESSAGE_64, ETHERNET_FRAME_EX):
            first += obj_size + obj_size % 4
        else:
            first += obj_size
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


def parse_can_message(obj_data: memoryview) -> CANMessage:
    channel, flags, dlc, can_id = CAN_MESSAGE_STRUCT.unpack_from(obj_data)
    data = obj_data[CAN_MESSAGE_STRUCT.size:CAN_MESSAGE_STRUCT.size + 8]
    return {"type": "can",
            "channel": channel,
            "dir": flags & DIR,
            "can_id": can_id,
            "dlc": dlc,
            "rtr": flags & RTR != 0,
            "fdf": False,
            "brs": False,
            "esi": False,
            "data": data}


def parse_can_fd_message(obj_data: memoryview) -> CANMessage:
    channel, flags, dlc, can_id, _, _, fd_flags, valid_data_bytes = CAN_FD_MESSAGE_STRUCT.unpack_from(obj_data)
    data = obj_data[CAN_FD_MESSAGE_STRUCT.size:CAN_FD_MESSAGE_STRUCT.size + valid_data_bytes]
    return {"type": "can",
            "channel": channel,
            "dir": flags & DIR,
            "can_id": can_id,
            "dlc": dlc,
            "rtr": flags & RTR != 0,
            "fdf": fd_flags & FDF != 0,
            "brs": fd_flags & BRS != 0,
            "esi": fd_flags & ESI != 0,
            "data": data}


def parse_can_fd_message_64(obj_data: memoryview) -> CANMessage:
    channel, dlc, valid_data_bytes, _, can_id, _, flags, _, _, _, _, _, dir, _, crc = CAN_FD_MESSAGE_64_STRUCT.unpack_from(obj_data)
    data = obj_data[CAN_FD_MESSAGE_64_STRUCT.size:CAN_FD_MESSAGE_64_STRUCT.size + valid_data_bytes]
    return {"type": "can",
            "channel": channel,
            "dir": dir,
            "can_id": can_id,
            "dlc": dlc,
            "rtr": flags & RTR_64 != 0,
            "fdf": flags & FDF_64 != 0,
            "brs": flags & BRS_64 != 0,
            "esi": flags & ESI_64 != 0,
            "data": data}


def parse_ethernet_frame(obj_data: memoryview) -> EthernetFrame:
    mac_sa, channel, mac_da, dir, eth_type, vlan_tpid, vlan_tci, frame_length = ETHERNET_FRAME_STRUCT.unpack_from(obj_data)
    data = obj_data[ETHERNET_FRAME_STRUCT.size:ETHERNET_FRAME_STRUCT.size + frame_length]
    return {"type": "ethernet",
            "channel": channel,
            "hw_channel": -1,
            "dir": dir,
            "mac_da": mac_da,
            "mac_sa": mac_sa,
            "eth_type": eth_type,
            "vlan_tpid": vlan_tpid,
            "vlan_pri": (vlan_tci >> 12) & 0x03,
            "vlan_id": vlan_tci & 0x3F,
            "data": data}


def parse_ethernet_frame_ex(obj_data: memoryview) -> EthernetFrame:
    _, flags, channel, hw_channel, _, checksum, dir, frame_length, frame_handle, _ = ETHERNET_FRAME_EX_STRUCT.unpack_from(obj_data)
    if frame_length <= 14:
        raise Exception("unexpected ethernet format")
    data = obj_data[ETHERNET_FRAME_EX_STRUCT.size:ETHERNET_FRAME_EX_STRUCT.size + frame_length]
    vlan_tpid, vlan_tci, eth_type = VLAN_TPID_TCI_TYPE.unpack_from(data, 12)
    if len(data) > 18 and (vlan_tpid == 0x8100 or vlan_tpid == 0x8800 or vlan_tpid == 0x9100):
        return {"type": "ethernet",
                "channel": channel,
                "hw_channel": hw_channel if flags & VALID_HW_CHANNEL != 0 else -1,
                "dir": dir,
                "mac_da": data[:6],
                "mac_sa": data[6:12],
                "vlan_tpid": vlan_tpid,
                "vlan_pri": (vlan_tci >> 12) & 0x03,
                "vlan_id": vlan_tci & 0xFFF,
                "eth_type": eth_type,
                "data": data[18:]}
    else:
        return {"type": "ethernet",
                "channel": channel,
                "hw_channel": hw_channel if flags & VALID_HW_CHANNEL != 0 else -1,
                "dir": dir,
                "mac_da": data[:6],
                "mac_sa": data[6:12],
                "vlan_tpid": -1,
                "vlan_pri": -1,
                "vlan_id": -1,
                "eth_type": vlan_tpid,
                "data": data[14:]}


def main():
    # filename = sys.argv[1]
    filename = r"C:\Users\k_hir\Projects\numba-test\test.blf"
    with open(filename, "rb") as fp:
        object_count, start_timestamp, stop_timestamp = parse_file_header(fp)
        it = parse_log_container(fp)
        t0 = time.time()
        for item in it:
            pass
        t1 = time.time()
        print(t1 - t0)
    with open(filename, "rb") as fp:
        with mmap(fp.fileno(), length=0, access=ACCESS_READ) as mm:
            object_count, start_timestamp, stop_timestamp = parse_file_header(mm)
            it = parse_log_container_mm(mm)
            t0 = time.time()
            for item in it:
                pass
            t1 = time.time()
            print(t1 - t0)


if __name__ == "__main__":
    main()
