#pragma once
#include <cstddef>
#include <cstdint>

// Result of one native read pass: number of chunks visited and the running sums
// of data_page_offset / total_compressed_size over the selected columns.
struct ReadResult {
    long long count;
    long long offset_sum;
    long long size_sum;
};

// Decode a full footer.core.current::FileMetaData (nested array-of-structs) and sum
// over `cols` for every row group.
ReadResult read_current_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols);

// Decode a full footer.core.soa::FileMetaData (struct-of-arrays) and sum over `cols`
// (column-major chunk index c*nr + g).
ReadResult read_soa_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols);

// Read a FlatBuffers SoA footer and sum data_page_offset / total_compressed_size over the selected
// columns (column-major chunk index c*nr + g). compressed != 0 first LZ4_FRAME-decompresses `data`
// into a raw_size buffer; compressed == 0 reads `data` directly (raw_size ignored). verify != 0 runs
// VerifyFileMetaDataBuffer before the scan. A corrupt frame or a failed verify returns {0,0,0} (the
// caller's cross-check against the other formats then trips, surfacing it).
ReadResult read_flatbuffers_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols,
                                   std::size_t raw_size, int nr, int verify, int compressed);
