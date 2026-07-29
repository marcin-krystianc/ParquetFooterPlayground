#include "shim.h"
#include "footer_core_current_types.h"
#include "footer_core_jumptable_types.h"
#include "footer_core_soa_types.h"
#include "footer_core_flatbuffers_generated.h"

#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/transport/TBufferTransports.h>
#include <lz4frame.h>
#include <memory>
#include <vector>

using apache::thrift::protocol::TCompactProtocol;
using apache::thrift::transport::TMemoryBuffer;

// Decode `obj` from `len` bytes at `data` using compact protocol. OBSERVE => no copy.
template <typename T>
static void decode(T& obj, const uint8_t* data, std::size_t len) {
    std::shared_ptr<TMemoryBuffer> buf(
        new TMemoryBuffer(const_cast<uint8_t*>(data), static_cast<uint32_t>(len), TMemoryBuffer::OBSERVE));
    TCompactProtocol proto(buf);
    obj.read(&proto);
}

ReadResult read_current_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols) {
    footer::core::current::FileMetaData md;
    decode(md, data, len);
    ReadResult r{0, 0, 0};
    for (const auto& rg : md.row_groups) {
        for (std::size_t k = 0; k < ncols; ++k) {
            const auto& m = rg.columns[cols[k]].meta_data;
            r.count++;
            r.offset_sum += m.data_page_offset;
            r.size_sum += m.total_compressed_size;
        }
    }
    return r;
}

ReadResult read_soa_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols) {
    footer::core::soa::FileMetaData md;
    decode(md, data, len);
    ReadResult r{0, 0, 0};
    const int nr = static_cast<int>(md.row_groups.num_rows.size());
    const auto& offs = md.chunks.data_page_offsets;
    const auto& sizes = md.chunks.total_compressed_sizes;
    for (std::size_t k = 0; k < ncols; ++k) {
        const int c = cols[k];
        for (int g = 0; g < nr; ++g) {
            const int i = c * nr + g;
            r.count++;
            r.offset_sum += offs[i];
            r.size_sum += sizes[i];
        }
    }
    return r;
}

ReadResult read_jumptable_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols,
                                 std::size_t hlen, int nc, int nr) {
    footer::core::jumptable::FileMetaData hdr;
    decode(hdr, data, hlen);
    const auto& rel = hdr.column_metadata_offsets;
    const std::size_t body_len = len - hlen;
    ReadResult r{0, 0, 0};
    for (int g = 0; g < nr; ++g) {
        for (std::size_t k = 0; k < ncols; ++k) {
            const int c = cols[k];
            const int j = g * nc + c;
            const int32_t start = rel[j];
            const int32_t end = (static_cast<std::size_t>(j + 1) < rel.size())
                                    ? rel[j + 1]
                                    : static_cast<int32_t>(body_len);
            footer::core::jumptable::ColumnMetaData m;
            decode(m, data + hlen + start, static_cast<std::size_t>(end - start));
            r.count++;
            r.offset_sum += m.data_page_offset;
            r.size_sum += m.total_compressed_size;
        }
    }
    return r;
}

ReadResult read_flatbuffers_native(const uint8_t* data, std::size_t len, const int* cols, std::size_t ncols,
                                   std::size_t raw_size, int nr, int verify, int compressed) {
    const uint8_t* fb = data;
    std::size_t fb_len = len;
    std::vector<uint8_t> raw;
    if (compressed) {
        raw.resize(raw_size);
        LZ4F_dctx* dctx = nullptr;
        if (LZ4F_isError(LZ4F_createDecompressionContext(&dctx, LZ4F_VERSION))) return ReadResult{0, 0, 0};
        std::size_t dst_size = raw_size, src_size = len;
        const std::size_t code = LZ4F_decompress(dctx, raw.data(), &dst_size, data, &src_size, nullptr);
        LZ4F_freeDecompressionContext(dctx);
        if (LZ4F_isError(code) || dst_size != raw_size) return ReadResult{0, 0, 0};
        fb = raw.data();
        fb_len = dst_size;
    }
    if (verify) {
        flatbuffers::Verifier v(fb, fb_len);
        if (!FooterCoreFb::VerifyFileMetaDataBuffer(v)) return ReadResult{0, 0, 0};
    }
    const auto* chunks = FooterCoreFb::GetFileMetaData(fb)->chunks();
    const auto* offs = chunks->data_page_offsets();
    const auto* sizes = chunks->total_compressed_sizes();
    ReadResult r{0, 0, 0};
    for (std::size_t k = 0; k < ncols; ++k) {
        const long long base = static_cast<long long>(cols[k]) * nr;
        for (int g = 0; g < nr; ++g) {
            const flatbuffers::uoffset_t i = static_cast<flatbuffers::uoffset_t>(base + g);
            r.count++;
            r.offset_sum += offs->Get(i);
            r.size_sum += sizes->Get(i);
        }
    }
    return r;
}
