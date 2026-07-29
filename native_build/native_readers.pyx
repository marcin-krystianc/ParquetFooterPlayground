# distutils: language = c++
# cython: language_level=3
from libc.stdint cimport uint8_t
from libcpp.vector cimport vector

cdef extern from "shim.h":
    cdef struct ReadResult:
        long long count
        long long offset_sum
        long long size_sum
    ReadResult read_current_native(const uint8_t* data, size_t length, const int* cols, size_t ncols)
    ReadResult read_soa_native(const uint8_t* data, size_t length, const int* cols, size_t ncols)
    ReadResult read_jumptable_native(const uint8_t* data, size_t length, const int* cols, size_t ncols,
                                     size_t hlen, int nc, int nr)
    ReadResult read_flatbuffers_native(const uint8_t* data, size_t length, const int* cols, size_t ncols,
                                       size_t raw_size, int nr, int verify, int compressed)

# Bump alongside benchmark NATIVE_ABI when any reader signature changes, so a stale .so rebuilds.
READER_ABI = 3


def current(const unsigned char[:] buf, cols):
    cdef vector[int] c = cols
    cdef ReadResult r = read_current_native(<const uint8_t*>&buf[0], buf.shape[0], c.data(), c.size())
    return (r.count, r.offset_sum, r.size_sum)


def soa(const unsigned char[:] buf, cols):
    cdef vector[int] c = cols
    cdef ReadResult r = read_soa_native(<const uint8_t*>&buf[0], buf.shape[0], c.data(), c.size())
    return (r.count, r.offset_sum, r.size_sum)


def jumptable(const unsigned char[:] buf, cols, size_t hlen, int nc, int nr):
    cdef vector[int] c = cols
    cdef ReadResult r = read_jumptable_native(<const uint8_t*>&buf[0], buf.shape[0], c.data(), c.size(), hlen, nc, nr)
    return (r.count, r.offset_sum, r.size_sum)


def flatbuffers(const unsigned char[:] buf, cols, size_t raw_size, int nr, int verify, int compressed):
    cdef vector[int] c = cols
    cdef ReadResult r = read_flatbuffers_native(<const uint8_t*>&buf[0], buf.shape[0], c.data(), c.size(), raw_size, nr, verify, compressed)
    return (r.count, r.offset_sum, r.size_sum)
