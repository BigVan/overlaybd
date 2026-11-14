#include "../../tools/vhdxconverter.h"
#include "../../overlaybd/lsmt/file.h"
#include <photon/photon.h>
#include <photon/fs/localfs.h>
#include <photon/common/alog.h>
#include <algorithm>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <vector>

int main(int argc, char **argv) {
    if (argc != 5) {
        fprintf(stderr, "Usage: verify_delta_reconversion input.vhdx reference.overlaybd new.data new.index\n");
        return 2;
    }
    set_log_output_level(ALOG_ERROR);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_DEFAULT))
        return 1;
    DEFER(photon::fini());
    VHDX::Reader reader;
    if (reader.open(argv[1]) || !reader.is_differencing())
        return 1;
    std::unique_ptr<photon::fs::IFile> data(photon::fs::open_localfile_adaptor(argv[3], O_RDWR | O_CREAT | O_EXCL, 0600));
    std::unique_ptr<photon::fs::IFile> index(photon::fs::open_localfile_adaptor(argv[4], O_RDWR | O_CREAT | O_EXCL, 0600));
    if (!data || !index)
        return 1;
    LSMT::LayerInfo info(data.get(), index.get());
    info.virtual_size = 0;
    std::unique_ptr<LSMT::IFileRW> layer(LSMT::create_file_rw(info, false));
    if (!layer || VHDX::convert_to_layer(reader, layer.get()))
        return 1;
    fflush(stdout);
    layer.reset();
    layer.reset(LSMT::open_file_rw(data.get(), index.get(), false));
    std::unique_ptr<photon::fs::IFile> blob(photon::fs::open_localfile_adaptor(argv[2], O_RDONLY));
    if (!layer || !blob)
        return 1;
    std::unique_ptr<LSMT::IFileRO> reference(LSMT::open_file_ro(blob.get(), false));
    struct stat status;
    if (!reference || reference->fstat(&status) || uint64_t(status.st_size) != reader.virtual_size())
        return 1;
    std::unique_ptr<LSMT::SegmentMapping[]> actual(layer->index()->dump());
    const auto *expected = reference->index()->buffer();
    size_t left = 0, right = 0;
    uint64_t left_offset = 0, right_offset = 0, compared = 0;
    std::vector<uint8_t> a(1024 * 1024), b(a.size());
    while (left < layer->index()->size() && right < reference->index()->size()) {
        uint64_t begin = actual[left].offset + left_offset;
        if (begin != expected[right].offset + right_offset) {
            fprintf(stderr, "Inherited coverage mismatch at sector %llu\n", (unsigned long long)begin);
            return 1;
        }
        uint64_t blocks = std::min(actual[left].length - left_offset, expected[right].length - right_offset);
        uint64_t bytes = blocks * 512;
        for (uint64_t offset = 0; offset < bytes; offset += a.size()) {
            auto count = std::min<uint64_t>(a.size(), bytes - offset);
            if (layer->pread(a.data(), count, begin * 512 + offset) != ssize_t(count) ||
                reference->pread(b.data(), count, begin * 512 + offset) != ssize_t(count) ||
                memcmp(a.data(), b.data(), count)) {
                fprintf(stderr, "Mapped content mismatch at byte %llu\n", (unsigned long long)(begin * 512 + offset));
                return 1;
            }
        }
        compared += bytes;
        left_offset += blocks;
        right_offset += blocks;
        if (left_offset == actual[left].length) {
            ++left;
            left_offset = 0;
        }
        if (right_offset == expected[right].length) {
            ++right;
            right_offset = 0;
        }
    }
    if (left != layer->index()->size() || right != reference->index()->size())
        return 1;
    printf("PASS: reconversion matches all inherited regions and %llu mapped bytes, virtual size %llu\n",
           (unsigned long long)compared, (unsigned long long)reader.virtual_size());
    return 0;
}
