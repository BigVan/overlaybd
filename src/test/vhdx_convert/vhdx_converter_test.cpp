#include <gtest/gtest.h>
#include <photon/photon.h>
#include <photon/fs/localfs.h>
#include <photon/fs/forwardfs.h>
#include <photon/common/alog.h>
#include "../../tools/vhdxconverter.h"
#include "../../overlaybd/lsmt/file.h"
#include "../../overlaybd/zfile/zfile.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace {

constexpr uint64_t MIB = 1024 * 1024;

void put(std::vector<uint8_t> &bytes, size_t offset, uint64_t value, size_t length) {
    for (size_t index = 0; index < length; ++index)
        bytes.at(offset + index) = value >> (8 * index);
}

void guid(std::vector<uint8_t> &bytes, size_t offset, const char *hex) {
    for (size_t index = 0; index < 16; ++index) {
        unsigned int value;
        ASSERT_EQ(sscanf(hex + 2 * index, "%2x", &value), 1);
        bytes.at(offset + index) = value;
    }
}

void checksum(std::vector<uint8_t> &bytes, size_t offset, size_t length) {
    put(bytes, offset + 4, 0, 4);
    uint32_t value = UINT32_MAX;
    for (size_t index = offset; index < offset + length; ++index) {
        value ^= bytes[index];
        for (size_t bit = 0; bit < 8; ++bit)
            value = (value >> 1) ^ ((value & 1) ? 0x82f63b78 : 0);
    }
    put(bytes, offset + 4, ~value, 4);
}

class Fixture : public ::testing::Test {
protected:
    std::string directory;
    std::vector<std::string> paths;
    std::vector<uint8_t> metadata;

    void SetUp() override {
        char name[] = "/tmp/overlaybd-vhdx-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        directory = name;
    }

    void TearDown() override {
        for (const auto &path : paths)
            unlink(path.c_str());
        rmdir(directory.c_str());
    }

    std::string path(const std::string &name) {
        auto result = directory + "/" + name;
        paths.push_back(result);
        return result;
    }

    void make_image(const std::string &filename, uint64_t size = 5 * MIB,
                    uint32_t sector = 512, uint32_t block = MIB, bool fixed = false,
                    std::map<uint64_t, uint8_t> states = {{0, 6}, {1, 2}, {2, 1}, {3, 3}, {4, 6}}) {
        metadata.assign(4 * MIB, 0);
        memcpy(metadata.data(), "vhdxfile", 8);
        for (size_t index = 0; index < 2; ++index) {
            auto offset = (index + 1) * 65536;
            memcpy(metadata.data() + offset, "head", 4);
            put(metadata, offset + 8, index + 1, 8);
            put(metadata, offset + 66, 1, 2);
            put(metadata, offset + 68, MIB, 4);
            put(metadata, offset + 72, MIB, 8);
            checksum(metadata, offset, 4096);
        }
        for (size_t index = 0; index < 2; ++index) {
            auto offset = (index + 3) * 65536;
            memcpy(metadata.data() + offset, "regi", 4);
            put(metadata, offset + 8, 2, 4);
            guid(metadata, offset + 16, "6677c22d23f600429d64115e9bfd4a08");
            put(metadata, offset + 32, 3 * MIB, 8);
            put(metadata, offset + 40, MIB, 4);
            put(metadata, offset + 44, 1, 4);
            guid(metadata, offset + 48, "06a27c8b90479a4bb8fe575f050f886e");
            put(metadata, offset + 64, 2 * MIB, 8);
            put(metadata, offset + 72, MIB, 4);
            put(metadata, offset + 76, 1, 4);
            checksum(metadata, offset, 65536);
        }
        memcpy(metadata.data() + 2 * MIB, "metadata", 8);
        put(metadata, 2 * MIB + 10, 5, 2);
        const char *keys[] = {
            "3767a1ca36fa434db3b633f0aa44e76b", "2442a52f1bcd7648b2115dbed83bf4b8",
            "1dbf41816fa90947ba47f233a8faab5f", "c748a3cd5d4471449cc9e9885251c556",
            "ab12cabee6b2234593efc309e000c746"};
        const uint32_t lengths[] = {8, 8, 4, 4, 16};
        for (size_t index = 0; index < 5; ++index) {
            size_t offset = 2 * MIB + 32 + index * 32;
            guid(metadata, offset, keys[index]);
            put(metadata, offset + 16, 65536 + index * 16, 4);
            put(metadata, offset + 20, lengths[index], 4);
            put(metadata, offset + 24, index == 0 ? 4 : 6, 4);
        }
        put(metadata, 2 * MIB + 65536, block, 4);
        put(metadata, 2 * MIB + 65540, fixed ? 1 : 0, 4);
        put(metadata, 2 * MIB + 65552, size, 8);
        put(metadata, 2 * MIB + 65568, sector, 4);
        put(metadata, 2 * MIB + 65584, 4096, 4);
        auto descriptor = ::open(filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0600);
        ASSERT_GE(descriptor, 0);
        uint64_t physical = 4 * MIB;
        uint64_t ratio = (uint64_t(1) << 23) * sector / block;
        for (const auto &entry : states) {
            if (entry.first * block >= size)
                continue;
            uint64_t value = entry.second;
            if (entry.second == 6) {
                value |= physical;
                std::vector<uint8_t> buffer(MIB, entry.first % 251 + 1);
                for (uint64_t position = 0; position < block; position += buffer.size())
                    ASSERT_EQ(::pwrite(descriptor, buffer.data(), buffer.size(), physical + position),
                              static_cast<ssize_t>(buffer.size()));
                physical += block;
            }
            put(metadata, 3 * MIB + 8 * (entry.first + entry.first / ratio), value, 8);
        }
        ASSERT_EQ(::pwrite(descriptor, metadata.data(), metadata.size(), 0),
                  static_cast<ssize_t>(metadata.size()));
        ASSERT_EQ(::close(descriptor), 0);
    }

    void rewrite(const std::string &filename) {
        auto descriptor = ::open(filename.c_str(), O_WRONLY);
        ASSERT_GE(descriptor, 0);
        ASSERT_EQ(::pwrite(descriptor, metadata.data(), metadata.size(), 0),
                  static_cast<ssize_t>(metadata.size()));
        ASSERT_EQ(::close(descriptor), 0);
    }

    void make_delta(const std::string &filename, uint32_t sector, uint32_t block,
                    const std::map<uint64_t, std::vector<uint64_t>> &partial) {
        put(metadata, 2 * MIB + 65540, 2, 4);
        put(metadata, 2 * MIB + 10, 6, 2);
        auto entry = 2 * MIB + 32 + 5 * 32;
        guid(metadata, entry, "2d5fd3a80bb34d45abf7d3d84834ab0c");
        put(metadata, entry + 16, 65792, 4);
        put(metadata, entry + 20, 256, 4);
        put(metadata, entry + 24, 4, 4);
        auto locator = 2 * MIB + 65792;
        guid(metadata, locator, "b7ef4ab09ed1814ab78925b8e9445913");
        put(metadata, locator + 18, 2, 2);
        std::pair<std::u16string, std::u16string> strings[] = {
            {u"relative_path", sector == 512 ? u"Z:\\missing-parent.vhdx" : u"/root/no-access/parent.vhdx"},
            {u"parent_linkage", u"{11111111-2222-3333-4444-555555555555}"}
        };
        uint64_t string_offset = 44;
        for (size_t item = 0; item < 2; ++item) {
            auto descriptor = locator + 20 + item * 12;
            const auto &key = strings[item].first;
            const auto &value = strings[item].second;
            put(metadata, descriptor, string_offset, 4);
            put(metadata, descriptor + 4, string_offset + key.size() * 2, 4);
            put(metadata, descriptor + 8, key.size() * 2, 2);
            put(metadata, descriptor + 10, value.size() * 2, 2);
            for (auto character : key + value) {
                put(metadata, locator + string_offset, character, 2);
                string_offset += 2;
            }
        }
        int descriptor = ::open(filename.c_str(), O_RDWR);
        ASSERT_GE(descriptor, 0);
        struct stat status;
        ASSERT_EQ(fstat(descriptor, &status), 0);
        uint64_t physical = (status.st_size + MIB - 1) / MIB * MIB;
        uint64_t ratio = (uint64_t(1) << 23) * sector / block;
        std::map<uint64_t, std::vector<uint8_t>> bitmaps;
        for (const auto &item : partial) {
            auto &bitmap = bitmaps[item.first / ratio];
            bitmap.resize(MIB);
            auto bat_offset = 3 * MIB + 8 * (item.first + item.first / ratio);
            metadata[bat_offset] = (metadata[bat_offset] & ~7) | 7;
            for (auto payload_sector : item.second) {
                uint64_t bit = (item.first % ratio) * (block / sector) + payload_sector;
                bitmap.at(bit / 8) |= 1 << (bit % 8);
            }
        }
        for (const auto &item : bitmaps) {
            put(metadata, 3 * MIB + 8 * (item.first * (ratio + 1) + ratio), physical | 6, 8);
            ASSERT_EQ(pwrite(descriptor, item.second.data(), MIB, physical), static_cast<ssize_t>(MIB));
            physical += MIB;
        }
        ASSERT_EQ(::close(descriptor), 0);
        rewrite(filename);
    }

    std::unique_ptr<photon::fs::IFile> commit_layer(LSMT::IFileRW *source,
                                                 const std::string &name, bool compressed) {
        auto output = std::unique_ptr<photon::fs::IFile>(photon::fs::open_localfile_adaptor(
            path(name).c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        if (!output)
            return nullptr;
        ZFile::CompressOptions options;
        options.verify = 1;
        ZFile::CompressArgs compression(options);
        std::unique_ptr<photon::fs::IFile> builder;
        if (compressed) {
            builder.reset(ZFile::new_zfile_builder(output.get(), &compression, false));
            if (!builder)
                return nullptr;
        }
        LSMT::CommitArgs args(compressed ? builder.get() : output.get());
        if (source->commit(args) || (builder && builder->close()))
            return nullptr;
        builder.reset();
        if (compressed) {
            auto decoded = ZFile::zfile_open_ro(output.get(), true, true);
            if (!decoded)
                return nullptr;
            output.release();
            output.reset(decoded);
        }
        return output;
    }

    std::unique_ptr<LSMT::IFileRW> layer(const std::string &name) {
        auto data = photon::fs::open_localfile_adaptor(path(name + ".data").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        auto index = photon::fs::open_localfile_adaptor(path(name + ".index").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600);
        LSMT::LayerInfo args(data, index);
        args.virtual_size = 0;
        args.rw_type = LSMT::RWType::Append;
        return std::unique_ptr<LSMT::IFileRW>(LSMT::create_file_rw(args, true));
    }

    std::unique_ptr<LSMT::IFileRW> reopen_layer(const std::string &name) {
        auto data = photon::fs::open_localfile_adaptor(path(name + ".data").c_str(), O_RDWR);
        auto index = photon::fs::open_localfile_adaptor(path(name + ".index").c_str(), O_RDWR);
        return std::unique_ptr<LSMT::IFileRW>(LSMT::open_file_rw(data, index, true));
    }

    void compare(const VHDX::Reader &reader, photon::fs::IFile *actual) {
        struct stat status;
        ASSERT_EQ(actual->fstat(&status), 0);
        ASSERT_EQ(static_cast<uint64_t>(status.st_size), reader.virtual_size());
        std::vector<uint8_t> expected(65536), result(65536);
        for (uint64_t offset = 0; offset < reader.virtual_size(); offset += expected.size()) {
            auto count = std::min<uint64_t>(expected.size(), reader.virtual_size() - offset);
            ASSERT_EQ(reader.read_data(offset, expected.data(), count), static_cast<ssize_t>(count));
            ASSERT_EQ(actual->pread(result.data(), count, offset), static_cast<ssize_t>(count));
            ASSERT_TRUE(std::equal(expected.begin(), expected.begin() + count, result.begin())) << offset;
        }
        auto layer_file = static_cast<LSMT::IFileRO *>(actual);
        if (layer_file->index()->size()) {
            ASSERT_LE(layer_file->index()->back().end() * 512, reader.virtual_size());
        }
    }
};

TEST_F(Fixture, FixedDynamicAndSectorSizes) {
    for (auto sector : {512U, 4096U}) {
        for (bool fixed : {false, true}) {
            auto source = path(std::to_string(sector) + std::to_string(fixed));
            make_image(source, 5 * MIB - sector, sector, MIB, fixed);
            VHDX::Reader reader;
            ASSERT_EQ(reader.open(source.c_str()), 0);
            ASSERT_EQ(reader.logical_sector_size(), sector);
            ASSERT_EQ(reader.virtual_size(), 5 * MIB - sector);
            std::vector<uint8_t> buffer(4096);
            ASSERT_EQ(reader.read_data(MIB - 512, buffer.data(), buffer.size()), 4096);
            EXPECT_TRUE(std::all_of(buffer.begin(), buffer.begin() + 512, [](uint8_t value) { return value == 1; }));
            EXPECT_TRUE(std::all_of(buffer.begin() + 512, buffer.end(), [](uint8_t value) { return value == 0; }));
            for (uint64_t block = 0; block < 5; ++block) {
                ASSERT_EQ(reader.read_data(block * MIB, buffer.data(), buffer.size()), 4096);
                uint8_t expected = block == 0 ? 1 : (block == 4 ? 5 : 0);
                EXPECT_TRUE(std::all_of(buffer.begin(), buffer.end(), [expected](uint8_t value) { return value == expected; }));
            }
            EXPECT_EQ(reader.read_data(reader.virtual_size(), buffer.data(), 512), 0);
        }
    }
}

TEST_F(Fixture, ChunkBoundaryAndSparseLargeDisk) {
    for (auto sector : {512U, 4096U}) {
        uint64_t ratio = (uint64_t(1) << 23) * sector / MIB;
        auto source = path(std::to_string(sector));
        make_image(source, (ratio + 2) * MIB, sector, MIB, false,
                   {{ratio - 1, 6}, {ratio, 6}, {ratio + 1, 6}});
        VHDX::Reader reader;
        ASSERT_EQ(reader.open(source.c_str()), 0);
        std::array<uint8_t, 1024> buffer;
        ASSERT_EQ(reader.read_data(ratio * MIB - 512, buffer.data(), buffer.size()), 1024);
        EXPECT_EQ(buffer[0], (ratio - 1) % 251 + 1);
        EXPECT_EQ(buffer[512], ratio % 251 + 1);
    }
    auto source = path("large");
    make_image(source, 1000ULL * 1024 * MIB, 512, 32 * MIB, false, {});
    VHDX::Reader reader;
    ASSERT_EQ(reader.open(source.c_str()), 0);
    auto target = layer("large");
    ASSERT_NE(target, nullptr);
    ASSERT_EQ(VHDX::convert_to_layer(reader, target.get()), 0);
    EXPECT_EQ(target->index()->size(), 0U);
    struct stat status;
    ASSERT_EQ(target->fstat(&status), 0);
    EXPECT_EQ(static_cast<uint64_t>(status.st_size), reader.virtual_size());
    std::array<uint8_t, 512> buffer;
    ASSERT_EQ(target->pread(buffer.data(), buffer.size(), reader.virtual_size() - 512), 512);
    EXPECT_TRUE(std::all_of(buffer.begin(), buffer.end(), [](uint8_t value) { return value == 0; }));
}

TEST_F(Fixture, HeaderAndRegionRecovery) {
    auto source = path("source");
    make_image(source);
    auto original = metadata;
    VHDX::Reader reader;
    for (size_t offset : {65536U, 131072U, 196608U, 262144U}) {
        metadata = original;
        metadata[offset + 4] ^= 1;
        rewrite(source);
        EXPECT_EQ(reader.open(source.c_str()), 0) << offset;
    }
    metadata = original;
    put(metadata, 131072 + 8, 1, 8);
    checksum(metadata, 131072, 4096);
    rewrite(source);
    EXPECT_NE(reader.open(source.c_str()), 0);
    metadata = original;
    put(metadata, 262144 + 8, 1, 4);
    checksum(metadata, 262144, 65536);
    rewrite(source);
    EXPECT_NE(reader.open(source.c_str()), 0);
    for (size_t offset : {65536U, 196608U}) {
        metadata = original;
        metadata[offset + 4] ^= 1;
        metadata[offset + 65536 + 4] ^= 1;
        rewrite(source);
        EXPECT_NE(reader.open(source.c_str()), 0);
    }
}

TEST_F(Fixture, RejectMalformedAndUnsupportedInputs) {
    auto source = path("source");
    make_image(source);
    auto original = metadata;
    const std::vector<std::pair<size_t, uint64_t>> corruptions = {
        {0, 0}, {2 * MIB, 0}, {2 * MIB + 10, 4},
        {2 * MIB + 65540, 2}, {2 * MIB + 65536, 3 * MIB},
        {2 * MIB + 65568, 1024}, {2 * MIB + 65584, 1024},
        {2 * MIB + 65552, 0}, {2 * MIB + 65552, UINT64_MAX},
        {2 * MIB + 32 + 16, 32}, {2 * MIB + 32 + 20, 7},
        {2 * MIB + 64 + 16, 65536}, {2 * MIB + 32 + 16, UINT32_MAX},
        {3 * MIB, 4}, {3 * MIB, 5}, {3 * MIB, 7},
        {3 * MIB, 2 * MIB + 6}, {3 * MIB, 32 * MIB + 6},
        {3 * MIB + 4 * 8, 4 * MIB + 6}};
    for (const auto &corruption : corruptions) {
        metadata = original;
        put(metadata, corruption.first, corruption.second, 8);
        rewrite(source);
        VHDX::Reader reader;
        EXPECT_NE(reader.open(source.c_str()), 0) << corruption.first << ":" << corruption.second;
    }
    metadata = original;
    metadata[131072 + 48] = 1;
    checksum(metadata, 131072, 4096);
    rewrite(source);
    VHDX::Reader reader;
    EXPECT_NE(reader.open(source.c_str()), 0);
    metadata = original;
    metadata[2 * MIB + 32] ^= 1;
    rewrite(source);
    EXPECT_NE(reader.open(source.c_str()), 0);
    metadata = original;
    memcpy(metadata.data() + 2 * MIB + 64, metadata.data() + 2 * MIB + 32, 16);
    rewrite(source);
    EXPECT_NE(reader.open(source.c_str()), 0);
    metadata = original;
    rewrite(source);
    ASSERT_EQ(truncate(source.c_str(), 6 * MIB - 512), 0);
    EXPECT_NE(reader.open(source.c_str()), 0);
}

TEST_F(Fixture, RejectsNonemptyAndStackedTargets) {
    auto source = path("source");
    make_image(source);
    VHDX::Reader reader;
    ASSERT_EQ(reader.open(source.c_str()), 0);
    auto target = layer("nonempty");
    ASSERT_NE(target, nullptr);
    ASSERT_EQ(target->update_vsize(MIB), 0);
    std::array<uint8_t, 512> buffer{};
    buffer.fill(123);
    ASSERT_EQ(target->pwrite(buffer.data(), buffer.size(), 0), 512);
    ASSERT_NE(VHDX::convert_to_layer(reader, target.get()), 0);
    struct stat status;
    ASSERT_EQ(target->fstat(&status), 0);
    EXPECT_EQ(status.st_size, static_cast<off_t>(MIB));
    buffer.fill(0);
    ASSERT_EQ(target->pread(buffer.data(), buffer.size(), 0), 512);
    EXPECT_EQ(buffer[0], 123);
    auto upper = layer("upper");
    ASSERT_NE(upper, nullptr);
    std::unique_ptr<LSMT::IFileRW> stacked(LSMT::stack_files(upper.get(), target.get(), false, false));
    ASSERT_NE(stacked, nullptr);
    EXPECT_NE(VHDX::convert_to_layer(reader, stacked.get()), 0);
    EXPECT_EQ(upper->index()->size(), 0U);
}

class FaultFile : public photon::fs::ForwardFile_Ownership {
public:
    bool short_payload = false;
    bool fail_sync = false;
    bool fail_header = false;
    bool fail_payload = false;
    explicit FaultFile(photon::fs::IFile *file) : photon::fs::ForwardFile_Ownership(file, true) {}

    ssize_t pwrite(const void *buffer, size_t count, off_t offset) override {
        if ((fail_header && offset == 0) || (fail_payload && offset >= 4096)) {
            errno = EIO;
            return -1;
        }
        if (short_payload && offset >= 4096)
            return m_file->pwrite(buffer, count / 2, offset);
        return m_file->pwrite(buffer, count, offset);
    }

    int fsync() override {
        if (fail_sync) {
            errno = EIO;
            return -1;
        }
        return m_file->fsync();
    }

};

TEST_F(Fixture, PropagatesReadWriteResizeAndSyncFailures) {
    auto source = path("source");
    make_image(source);
    VHDX::Reader reader;
    ASSERT_EQ(reader.open(source.c_str()), 0);
    for (int failure = 0; failure < 4; ++failure) {
        auto data = new FaultFile(photon::fs::open_localfile_adaptor(
            path(std::to_string(failure) + ".data").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        auto index = new FaultFile(photon::fs::open_localfile_adaptor(
            path(std::to_string(failure) + ".index").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        LSMT::LayerInfo info(data, index);
        info.virtual_size = 0;
        std::unique_ptr<LSMT::IFileRW> target(LSMT::create_file_rw(info, true));
        ASSERT_NE(target, nullptr);
        data->short_payload = failure == 0;
        data->fail_header = failure == 1;
        data->fail_sync = failure == 2;
        index->fail_sync = failure == 3;
        EXPECT_NE(VHDX::convert_to_layer(reader, target.get()), 0) << failure;
    }
    auto target = layer("truncated");
    ASSERT_NE(target, nullptr);
    ASSERT_EQ(truncate(source.c_str(), 4 * MIB + 512), 0);
    EXPECT_NE(VHDX::convert_to_layer(reader, target.get()), 0);
    EXPECT_EQ(target->index()->size(), 0U);
}

TEST_F(Fixture, RejectsInvalidRegionsParentLocatorAndShortBat) {
    auto source = path("source");
    make_image(source);
    auto original = metadata;
    VHDX::Reader reader;
    for (int failure = 0; failure < 6; ++failure) {
        metadata = original;
        for (size_t table : {196608U, 262144U}) {
            if (failure == 0)
                metadata[table + 16] ^= 1;
            if (failure == 1)
                memcpy(metadata.data() + table + 48, metadata.data() + table + 16, 16);
            if (failure == 2)
                put(metadata, table + 64, 3 * MIB, 8);
            if (failure == 3)
                put(metadata, table + 64, UINT64_MAX - MIB + 1, 8);
            checksum(metadata, table, 65536);
        }
        if (failure == 4)
            put(metadata, 2 * MIB + 65552, 1024ULL * 1024 * MIB, 8);
        if (failure == 5) {
            put(metadata, 2 * MIB + 10, 6, 2);
            guid(metadata, 2 * MIB + 32 + 5 * 32, "2d5fd3a80bb34d45abf7d3d84834ab0c");
        }
        rewrite(source);
        EXPECT_NE(reader.open(source.c_str()), 0) << failure;
    }
}

TEST_F(Fixture, ConversionCommitAndZfileRoundTrip) {
    auto source = path("source");
    for (uint32_t sector : {512U, 4096U}) {
        make_image(source, 5 * MIB - 4096, sector);
        VHDX::Reader reader;
        ASSERT_EQ(reader.open(source.c_str()), 0);
        auto name = std::to_string(sector);
        auto target = layer(name);
        ASSERT_NE(target, nullptr);
        ASSERT_EQ(VHDX::convert_to_layer(reader, target.get()), 0);
        compare(reader, target.get());
        std::unique_ptr<photon::fs::IFile> output(photon::fs::open_localfile_adaptor(
            path(name + ".commit").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        ASSERT_NE(output, nullptr);
        LSMT::CommitArgs args(output.get());
        ASSERT_EQ(target->commit(args), 0);
        std::unique_ptr<LSMT::IFileRO> committed(LSMT::open_file_ro(output.get(), false));
        ASSERT_NE(committed, nullptr);
        compare(reader, committed.get());
        std::unique_ptr<photon::fs::IFile> compressed(photon::fs::open_localfile_adaptor(
            path(name + ".zfile").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        ASSERT_NE(compressed, nullptr);
        ZFile::CompressOptions options;
        options.verify = 1;
        ZFile::CompressArgs compression(options);
        ASSERT_EQ(output->lseek(0, SEEK_SET), 0);
        ASSERT_EQ(ZFile::zfile_compress(output.get(), compressed.get(), &compression), 0);
        std::unique_ptr<photon::fs::IFile> decompressed(ZFile::zfile_open_ro(compressed.get(), true, false));
        ASSERT_NE(decompressed, nullptr);
        std::unique_ptr<LSMT::IFileRO> compressed_layer(LSMT::open_file_ro(decompressed.get(), false));
        ASSERT_NE(compressed_layer, nullptr);
        compare(reader, compressed_layer.get());
    }
}

TEST_F(Fixture, ShuffledPayloadsAreAppendedInLogicalOrder) {
    for (uint32_t sector : {512U, 4096U}) {
        for (bool delta : {false, true}) {
            auto prefix = std::to_string(sector) + (delta ? ".delta" : ".disk");
            auto source = path(prefix + ".vhdx");
            uint64_t size = 6 * MIB + 4096;
            make_image(source, size, sector, MIB, false,
                       {{0, 6}, {1, 6}, {2, 2}, {4, 6}, {5, 6}, {6, 6}});
            // Reverse physical payload order while keeping the logical BAT slots.
            const uint64_t blocks[] = {0, 1, 4, 5, 6};
            const uint8_t values[] = {7, 6, 5, 2, 1};
            for (size_t index = 0; index < 5; ++index)
                put(metadata, 3 * MIB + blocks[index] * 8, (8 - index) * MIB | 6, 8);
            rewrite(source);
            std::vector<uint8_t> expected(size, 0);
            for (size_t index = 0; index < 5; ++index) {
                uint64_t begin = blocks[index] * MIB;
                std::fill(expected.begin() + begin,
                          expected.begin() + std::min(begin + MIB, size), values[index]);
            }
            if (delta) {
                make_delta(source, sector, MIB,
                           {{1, {0, 7, 8, MIB / sector - 1}}, {6, {0}}});
                std::fill(expected.begin() + MIB, expected.begin() + 2 * MIB, 0);
                for (uint64_t owned : {uint64_t(0), uint64_t(7), uint64_t(8), MIB / sector - 1})
                    std::fill(expected.begin() + MIB + owned * sector,
                              expected.begin() + MIB + (owned + 1) * sector, 6);
                std::fill(expected.begin() + 6 * MIB + sector, expected.end(), 0);
            }
            VHDX::Reader reader;
            ASSERT_EQ(reader.open(source.c_str()), 0);
            auto target = layer(prefix);
            ASSERT_NE(target, nullptr);
            ASSERT_EQ(VHDX::convert_to_layer(reader, target.get()), 0);
            std::vector<uint8_t> actual(size);
            ASSERT_EQ(target->pread(actual.data(), size, 0), static_cast<ssize_t>(size));
            EXPECT_EQ(actual, expected);
            std::vector<LSMT::SegmentMapping> mappings(target->index()->size());
            ASSERT_EQ(target->index()->lookup({0, static_cast<uint32_t>(size / 512)},
                                             mappings.data(), mappings.size()), mappings.size());
            uint64_t logical_end = 0, physical_end = 0;
            bool found_zero = false;
            for (const auto &mapping : mappings) {
                EXPECT_GE(uint64_t(mapping.offset), logical_end);
                EXPECT_GE(uint64_t(mapping.moffset), physical_end);
                logical_end = mapping.end();
                physical_end = mapping.mend();
                found_zero |= mapping.zeroed;
            }
            EXPECT_EQ(found_zero, delta);
            for (bool compressed : {false, true}) {
                auto file = commit_layer(target.get(), prefix + std::to_string(compressed), compressed);
                ASSERT_NE(file, nullptr);
                std::unique_ptr<LSMT::IFileRO> committed(LSMT::open_file_ro(file.get(), false));
                ASSERT_NE(committed, nullptr);
                ASSERT_EQ(committed->pread(actual.data(), size, 0), static_cast<ssize_t>(size));
                EXPECT_EQ(actual, expected);
            }
        }
    }
}

TEST_F(Fixture, ScansAllSectorBitmapsBeforeConversion) {
    for (uint32_t sector : {512U, 4096U}) {
        auto prefix = std::to_string(sector);
        auto source = path(prefix + ".vhdx");
        uint64_t ratio = (uint64_t(1) << 23) * sector / MIB;
        make_image(source, (ratio + 1) * MIB, sector, MIB, false,
                   {{0, 6}, {ratio - 1, 6}, {ratio, 6}});
        make_delta(source, sector, MIB,
                   {{ratio - 1, {MIB / sector - 1}}, {ratio, {0, 7, 8}}});
        VHDX::Reader reader;
        ASSERT_EQ(reader.open(source.c_str()), 0);
        // Fault injection: remove both bitmaps after open. Conversion must consume
        // the already prepared index, without consulting source metadata again.
        int descriptor = ::open(source.c_str(), O_WRONLY);
        ASSERT_GE(descriptor, 0);
        std::vector<uint8_t> zeros(MIB);
        for (uint64_t offset : {7 * MIB, 8 * MIB})
            ASSERT_EQ(::pwrite(descriptor, zeros.data(), zeros.size(), offset), static_cast<ssize_t>(MIB));
        ASSERT_EQ(::close(descriptor), 0);
        auto target = layer(prefix);
        ASSERT_NE(target, nullptr);
        ASSERT_EQ(VHDX::convert_to_layer(reader, target.get()), 0);
        EXPECT_EQ(target->index()->block_count(), (MIB + 4 * sector) / 512);
        std::vector<uint8_t> actual(sector);
        for (uint64_t offset : {uint64_t(0), ratio * MIB - sector, ratio * MIB,
                                ratio * MIB + 7 * sector, ratio * MIB + 8 * sector}) {
            ASSERT_EQ(target->pread(actual.data(), sector, offset), sector);
            uint8_t expected = (offset / MIB) % 251 + 1;
            EXPECT_TRUE(std::all_of(actual.begin(), actual.end(),
                                   [expected](uint8_t value) { return value == expected; }));
        }
        LSMT::SegmentMapping mapping;
        EXPECT_EQ(target->index()->lookup({(ratio * MIB + sector) / 512, sector / 512},
                                         &mapping, 1), 0U);
    }
}

TEST_F(Fixture, IndependentDeltaConversionAndThreeLayerStack) {
    for (uint32_t sector : {512U, 4096U}) {
        uint64_t size = 6 * MIB + 4096;
        auto prefix = std::to_string(sector);
        auto base_path = path(prefix + ".base.vhdx");
        make_image(base_path, size, sector, 2 * MIB, false, {{0, 6}, {1, 6}, {2, 6}, {3, 6}});
        VHDX::Reader base_reader;
        ASSERT_EQ(base_reader.open(base_path.c_str()), 0);
        std::vector<uint8_t> expected(size), actual(size);
        ASSERT_EQ(base_reader.read_data(0, expected.data(), size), static_cast<ssize_t>(size));
        auto base = layer(prefix + ".base");
        ASSERT_EQ(VHDX::convert_to_layer(base_reader, base.get()), 0);

        auto child_path = path(prefix + ".child.vhdx");
        make_image(child_path, size, sector, MIB, false, {{1, 6}, {2, 2}, {3, 1}, {4, 3}, {5, 6}, {6, 6}});
        make_delta(child_path, sector, MIB, {{5, {0, 7, 8, MIB / sector - 1}}, {6, {0}}});
        int descriptor = ::open(child_path.c_str(), O_RDWR);
        ASSERT_GE(descriptor, 0);
        std::vector<uint8_t> zeros(MIB);
        ASSERT_EQ(pwrite(descriptor, zeros.data(), MIB, 4 * MIB), static_cast<ssize_t>(MIB));
        ASSERT_EQ(pwrite(descriptor, zeros.data(), sector, 6 * MIB), sector);
        ::close(descriptor);
        VHDX::Reader child_reader;
        ASSERT_EQ(child_reader.open(child_path.c_str()), 0);
        ASSERT_TRUE(child_reader.is_differencing());
        EXPECT_EQ(child_reader.read_data(0, actual.data(), sector), -1);
        auto child = layer(prefix + ".child");
        ASSERT_EQ(VHDX::convert_to_layer(child_reader, child.get()), 0);
        LSMT::SegmentMapping mapping[4];
        EXPECT_EQ(child->index()->lookup({0, 1}, mapping, 4), 0U);
        EXPECT_EQ(child->index()->lookup({(5 * MIB + sector) / 512, 1}, mapping, 4), 0U);
        ASSERT_EQ(child->index()->lookup({MIB / 512, 4 * MIB / 512}, mapping, 4), 1U);
        EXPECT_TRUE(mapping[0].zeroed);
        EXPECT_EQ(mapping[0].length, 4 * MIB / 512);
        child.reset();
        child = reopen_layer(prefix + ".child");
        ASSERT_NE(child, nullptr);
        EXPECT_EQ(child->index()->lookup({0, 1}, mapping, 4), 0U);
        EXPECT_EQ(child->index()->lookup({(5 * MIB + sector) / 512, 1}, mapping, 4), 0U);
        std::fill(expected.begin() + MIB, expected.begin() + 5 * MIB, 0);
        for (uint64_t owned : {uint64_t(0), uint64_t(7), uint64_t(8), MIB / sector - 1})
            std::fill(expected.begin() + 5 * MIB + owned * sector,
                      expected.begin() + 5 * MIB + (owned + 1) * sector, 6);
        std::fill(expected.begin() + 6 * MIB, expected.begin() + 6 * MIB + sector, 0);

        auto third_path = path(prefix + ".third.vhdx");
        make_image(third_path, size, sector, 2 * MIB, false, {{0, 6}, {2, 2}});
        make_delta(third_path, sector, 2 * MIB, {{0, {1}}});
        VHDX::Reader third_reader;
        ASSERT_EQ(third_reader.open(third_path.c_str()), 0);
        auto third = layer(prefix + ".third");
        ASSERT_EQ(VHDX::convert_to_layer(third_reader, third.get()), 0);
        for (bool compressed : {false, true}) {
            auto suffix = prefix + std::to_string(compressed);
            auto base_file = commit_layer(base.get(), suffix + ".base.blob", compressed);
            auto child_file = commit_layer(child.get(), suffix + ".child.blob", compressed);
            auto third_file = commit_layer(third.get(), suffix + ".third.blob", compressed);
            ASSERT_NE(base_file, nullptr);
            ASSERT_NE(child_file, nullptr);
            ASSERT_NE(third_file, nullptr);
            photon::fs::IFile *files[] = {base_file.get(), child_file.get(), third_file.get()};
            std::unique_ptr<LSMT::IFileRO> two(LSMT::open_files_ro(files, 2, false));
            ASSERT_NE(two, nullptr);
            ASSERT_EQ(two->pread(actual.data(), size, 0), static_cast<ssize_t>(size));
            EXPECT_EQ(actual, expected);
            std::unique_ptr<LSMT::IFileRO> three(LSMT::open_files_ro(files, 3, false));
            ASSERT_NE(three, nullptr);
            auto expected_third = expected;
            std::fill(expected_third.begin() + sector, expected_third.begin() + 2 * sector, 1);
            std::fill(expected_third.begin() + 4 * MIB, expected_third.begin() + 6 * MIB, 0);
            ASSERT_EQ(three->pread(actual.data(), size, 0), static_cast<ssize_t>(size));
            EXPECT_EQ(actual, expected_third);
        }
    }
}

TEST_F(Fixture, DeltaChunkBoundaryAndBitmapValidation) {
    for (uint32_t sector : {512U, 4096U}) {
        auto prefix = std::to_string(sector);
        auto source = path(prefix + ".boundary.vhdx");
        uint64_t ratio = (uint64_t(1) << 23) * sector / MIB;
        make_image(source, (ratio + 1) * MIB, sector, MIB, false, {{ratio - 1, 6}, {ratio, 6}});
        make_delta(source, sector, MIB, {{ratio - 1, {MIB / sector - 1}}, {ratio, {0, 1, 8}}});
        auto original = metadata;
        VHDX::Reader reader;
        ASSERT_EQ(reader.open(source.c_str()), 0);
        auto target = layer(prefix + ".boundary");
        ASSERT_EQ(VHDX::convert_to_layer(reader, target.get()), 0);
        EXPECT_EQ(target->index()->block_count(), 4U * sector / 512);
        std::vector<uint8_t> data(2 * sector);
        ASSERT_EQ(target->pread(data.data(), data.size(), ratio * MIB - sector), 2 * sector);
        EXPECT_EQ(data[0], (ratio - 1) % 251 + 1);
        EXPECT_EQ(data[sector], ratio % 251 + 1);
        for (uint64_t bad : {uint64_t(0), uint64_t(7), uint64_t(2 * MIB + 6),
                             uint64_t(64 * MIB + 6)}) {
            metadata = original;
            put(metadata, 3 * MIB + ratio * 8, bad, 8);
            rewrite(source);
            EXPECT_NE(reader.open(source.c_str()), 0);
        }
        metadata = original;
        rewrite(source);
        struct stat status;
        ASSERT_EQ(stat(source.c_str(), &status), 0);
        ASSERT_EQ(truncate(source.c_str(), status.st_size - 512), 0);
        EXPECT_NE(reader.open(source.c_str()), 0);
    }
}

TEST_F(Fixture, RejectsTruncatedBitmapBeforeConversionAndPropagatesZeroFailures) {
    auto source = path("bitmap-read.vhdx");
    make_image(source, MIB, 4096, MIB, false, {{0, 6}});
    make_delta(source, 4096, MIB, {{0, {0}}});
    VHDX::Reader reader;
    struct stat status;
    ASSERT_EQ(stat(source.c_str(), &status), 0);
    ASSERT_EQ(truncate(source.c_str(), status.st_size - 512), 0);
    auto target = layer("bitmap-read");
    EXPECT_NE(reader.open(source.c_str()), 0);
    EXPECT_NE(VHDX::convert_to_layer(reader, target.get()), 0);
    EXPECT_EQ(target->index()->size(), 0U);
    ASSERT_EQ(target->fstat(&status), 0);
    EXPECT_EQ(status.st_size, 0);

    source = path("zero-failure.vhdx");
    make_image(source, MIB, 4096, MIB, false, {{0, 2}});
    make_delta(source, 4096, MIB, {});
    ASSERT_EQ(reader.open(source.c_str()), 0);
    auto data = new FaultFile(photon::fs::open_localfile_adaptor(
        path("zero-failure.data").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
    auto index = new FaultFile(photon::fs::open_localfile_adaptor(
        path("zero-failure.index").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
    LSMT::LayerInfo info(data, index);
    info.virtual_size = 0;
    target.reset(LSMT::create_file_rw(info, true));
    ASSERT_NE(target, nullptr);
    index->fail_payload = true;
    EXPECT_NE(VHDX::convert_to_layer(reader, target.get()), 0);
    EXPECT_EQ(errno, EIO);
    EXPECT_EQ(target->index()->size(), 0U);
}

TEST_F(Fixture, RejectsUnalignedCapacityBeforeWriting) {
    auto source = path("unaligned.vhdx");
    make_image(source, MIB + 512);
    VHDX::Reader reader;
    ASSERT_EQ(reader.open(source.c_str()), 0);
    auto target = layer("unaligned");
    ASSERT_NE(target, nullptr);
    EXPECT_NE(VHDX::convert_to_layer(reader, target.get()), 0);
    EXPECT_EQ(target->index()->size(), 0U);
    struct stat status;
    ASSERT_EQ(target->fstat(&status), 0);
    EXPECT_EQ(status.st_size, 0);
}

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    set_log_output_level(ALOG_ERROR);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_DEFAULT) != 0)
        return 1;
    auto result = RUN_ALL_TESTS();
    photon::fini();
    return result;
}
