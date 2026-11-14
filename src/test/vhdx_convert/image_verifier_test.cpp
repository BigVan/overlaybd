#include "image_verifier.h"
#include "../../overlaybd/lsmt/file.h"
#include "../../overlaybd/zfile/zfile.h"
#include <gtest/gtest.h>
#include <photon/photon.h>
#include <photon/fs/localfs.h>
#include <photon/common/alog.h>
#include <algorithm>
#include <fcntl.h>
#include <memory>
#include <sys/wait.h>
#include <unistd.h>

namespace {
using namespace ImageVerifier;
constexpr size_t MIB = 1024 * 1024;
struct Patch {
    uint64_t offset;
    std::vector<uint8_t> bytes;
    bool zero = false;
};
struct CloseFile {
    void operator()(FILE *file) const { fclose(file); }
};

class VerifierTest : public ::testing::Test {
protected:
    std::string directory;
    std::vector<std::string> paths;
    void SetUp() override {
        char name[] = "/tmp/overlaybd-verifier-XXXXXX";
        ASSERT_NE(mkdtemp(name), nullptr);
        directory = name;
    }
    void TearDown() override {
        for (const auto &path : paths)
            unlink(path.c_str());
        rmdir(directory.c_str());
    }
    std::string path(const std::string &name) {
        paths.push_back(directory + "/" + name);
        return paths.back();
    }
    std::string raw(const std::string &name, const std::vector<uint8_t> &bytes) {
        auto filename = path(name);
        FILE *file = fopen(filename.c_str(), "wb");
        EXPECT_NE(file, nullptr);
        if (file) {
            EXPECT_EQ(fwrite(bytes.data(), 1, bytes.size(), file), bytes.size());
            EXPECT_EQ(fclose(file), 0);
        }
        return filename;
    }
    std::string blob(const std::string &name, uint64_t size, const std::vector<Patch> &patches,
                     bool compressed = false) {
        using File = std::unique_ptr<photon::fs::IFile>;
        File data(photon::fs::open_localfile_adaptor(path(name + ".data").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        File index(photon::fs::open_localfile_adaptor(path(name + ".index").c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        LSMT::LayerInfo info(data.get(), index.get());
        info.virtual_size = size;
        std::unique_ptr<LSMT::IFileRW> layer(LSMT::create_file_rw(info, false));
        EXPECT_NE(layer, nullptr);
        if (!layer)
            return {};
        for (const auto &patch : patches) {
            if (patch.zero)
                EXPECT_EQ(layer->fallocate(FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE,
                                          patch.offset, patch.bytes.size()), 0);
            else
                EXPECT_EQ(layer->pwrite(patch.bytes.data(), patch.bytes.size(), patch.offset),
                          ssize_t(patch.bytes.size()));
        }
        EXPECT_EQ(layer->update_vsize(size), 0);
        auto filename = path(name + ".blob");
        File output(photon::fs::open_localfile_adaptor(filename.c_str(), O_RDWR | O_CREAT | O_EXCL, 0600));
        ZFile::CompressOptions options;
        options.verify = 1;
        ZFile::CompressArgs compression(options);
        File builder;
        if (compressed)
            builder.reset(ZFile::new_zfile_builder(output.get(), &compression, false));
        LSMT::CommitArgs commit(compressed ? builder.get() : output.get());
        EXPECT_EQ(layer->commit(commit), 0);
        if (builder) {
            EXPECT_EQ(builder->close(), 0);
        }
        return filename;
    }
    std::unique_ptr<FILE, CloseFile> stream(uint64_t size, const std::vector<uint8_t> &data,
                                          bool extra = false) {
        std::unique_ptr<FILE, CloseFile> file(tmpfile());
        EXPECT_NE(file, nullptr);
        if (!file)
            return file;
        fwrite("VHDXRAW1", 1, 8, file.get());
        for (size_t index = 0; index < 8; ++index)
            fputc((size >> (8 * index)) & 255, file.get());
        fwrite(data.data(), 1, data.size(), file.get());
        if (extra)
            fputc(1, file.get());
        rewind(file.get());
        return file;
    }
    Request request(const std::string &reference, std::vector<std::vector<std::string>> groups) {
        Request result;
        result.reference.paths = {reference};
        result.groups = std::move(groups);
        return result;
    }
};

TEST_F(VerifierTest, RawAndMixedStacksIncludeHolesAndTail) {
    size_t size = 2 * MIB + 512;
    std::vector<Patch> patches = {{0, std::vector<uint8_t>(MIB, 0x19)},
                                  {2 * MIB, std::vector<uint8_t>(512, 0x32)}};
    std::vector<uint8_t> expected(size, 0);
    std::fill(expected.begin(), expected.begin() + MIB, 0x19);
    std::fill(expected.end() - 512, expected.end(), 0x32);
    auto plain = blob("base", size, patches);
    auto zipped = blob("zbase", size, patches, true);
    auto result = verify(request(raw("base.raw", expected), {{plain}, {zipped}}));
    ASSERT_TRUE(result.success()) << describe_error(result);
    EXPECT_EQ(result.compared_bytes, size);
    EXPECT_EQ(result.view_count, 2U);
    std::vector<Patch> changes = {{0, std::vector<uint8_t>(512), true},
                                 {MIB - 512, std::vector<uint8_t>(512, 0x77)}};
    auto delta = blob("delta", size, changes);
    auto zdelta = blob("zdelta", size, changes, true);
    std::fill(expected.begin(), expected.begin() + 512, 0);
    std::fill(expected.begin() + MIB - 512, expected.begin() + MIB, 0x77);
    result = verify(request(raw("stack.raw", expected), {{plain, zdelta}, {zipped, delta}}));
    EXPECT_TRUE(result.success()) << describe_error(result);
    EXPECT_EQ(result.compared_bytes, size);
}

TEST_F(VerifierTest, StreamIsReadOnceForMultipleViewsAndRemainsOpen) {
    std::vector<uint8_t> expected(MIB + 512, 0x41);
    auto layer = blob("layer", expected.size(), {{0, expected}});
    auto reference = stream(expected.size(), expected);
    auto req = request({}, {{layer}, {layer}});
    req.reference.kind = ReferenceKind::Stream;
    req.reference.stream = reference.get();
    req.reference.label = "known fixture";
    auto result = verify(req);
    EXPECT_TRUE(result.success()) << describe_error(result);
    EXPECT_EQ(result.reference_description, "known fixture");
    EXPECT_EQ(result.compared_bytes, expected.size());
    EXPECT_NE(fcntl(fileno(reference.get()), F_GETFD), -1);
    rewind(reference.get());
    EXPECT_EQ(fgetc(reference.get()), 'V');
}

TEST_F(VerifierTest, EmptyRawAndInvalidRequests) {
    auto empty = blob("empty", 0, {});
    auto result = verify(request(raw("empty.raw", {}), {{empty}, {empty, empty}}));
    EXPECT_TRUE(result.success()) << describe_error(result);
    EXPECT_EQ(result.compared_bytes, 0U);
    EXPECT_EQ(verify(Request{}).error, Error::InvalidRequest);
    EXPECT_EQ(verify(request("missing", {{}})).error, Error::InvalidRequest);
    EXPECT_EQ(verify(request("missing", {{empty}})).error, Error::ReferenceOpen);
    auto req = request({}, {{empty}});
    req.reference.kind = ReferenceKind::Stream;
    EXPECT_EQ(verify(req).error, Error::InvalidRequest);
}

TEST_F(VerifierTest, PreflightRejectsSizeRangeAndMissingPathsWithoutReadingPayload) {
    auto reference = stream(512, {}); // Payload intentionally missing.
    auto req = request({}, {{blob("valid-size", 512, {})}, {blob("wrong-size", 1024, {})}});
    req.reference.kind = ReferenceKind::Stream;
    req.reference.stream = reference.get();
    auto mismatch = verify(req);
    EXPECT_EQ(mismatch.error, Error::LayerSize);
    EXPECT_EQ(mismatch.view_index, 1U);
    EXPECT_EQ(ftell(reference.get()), 16);
    req.groups = {{path("missing")}};
    rewind(reference.get());
    EXPECT_EQ(verify(req).error, Error::LayerOpen);
    req.groups = {{blob("out-of-range", 512, {{512, std::vector<uint8_t>(512, 3)}})}};
    rewind(reference.get());
    auto result = verify(req);
    EXPECT_EQ(result.error, Error::LayerRange) << describe_error(result);
    EXPECT_EQ(result.view_index, 0U);
    EXPECT_EQ(ftell(reference.get()), 16);
}

TEST_F(VerifierTest, ReportsFirstMismatchAndSuccessfullyComparedPrefix) {
    std::vector<uint8_t> expected(2 * MIB + 512, 4);
    auto actual = expected;
    actual[MIB + 123] = 9;
    actual[MIB + 124] = 10;
    auto layer = blob("wrong", expected.size(), {{0, actual}});
    auto result = verify(request(raw("reference", expected), {{layer}}));
    EXPECT_EQ(result.error, Error::Mismatch);
    EXPECT_EQ(result.offset, MIB + 123);
    EXPECT_EQ(result.compared_bytes, MIB);
    EXPECT_EQ(result.expected, 4);
    EXPECT_EQ(result.actual, 9);
    EXPECT_EQ(result.path, layer);
    EXPECT_EQ(result.view_index, 0U);
}

TEST_F(VerifierTest, DetectsReferenceAndLayerReadsFailingAfterPreflight) {
    std::vector<uint8_t> bytes(3 * MIB, 0x29);
    auto reference = raw("reference", bytes);
    auto layer = blob("layer", bytes.size(), {{0, bytes}});
    auto result = verify(request(reference, {{layer}}), [&](const Progress &) {
        EXPECT_EQ(truncate(reference.c_str(), MIB), 0);
    });
    EXPECT_EQ(result.error, Error::ReferenceRead);
    EXPECT_EQ(result.offset, MIB);
    reference = raw("reference2", bytes);
    result = verify(request(reference, {{layer}}), [&](const Progress &) {
        EXPECT_EQ(truncate(layer.c_str(), 4096 + MIB), 0);
    });
    // The plain LSMT reader pads a truncated payload with zeros: compare must still fail.
    EXPECT_EQ(result.error, Error::Mismatch) << describe_error(result);
    EXPECT_EQ(result.offset, MIB);
    EXPECT_EQ(result.view_index, 0U);
    EXPECT_EQ(result.path, layer);
    uint32_t seed = 17;
    for (auto &byte : bytes) {
        seed ^= seed << 13;
        seed ^= seed >> 17;
        seed ^= seed << 5;
        byte = seed;
    }
    reference = raw("reference3", bytes);
    auto zipped = blob("compressed", bytes.size(), {{0, bytes}}, true);
    result = verify(request(reference, {{zipped}}), [&](const Progress &) {
        EXPECT_EQ(truncate(zipped.c_str(), 0), 0);
    });
    EXPECT_EQ(result.error, Error::LayerRead) << describe_error(result);
    EXPECT_EQ(result.offset, MIB);
    EXPECT_EQ(result.path, zipped);
}

TEST_F(VerifierTest, RejectsBadStreamHeadersCapacityTruncationAndTrailingData) {
    std::vector<uint8_t> bytes(MIB + 512, 0x34);
    auto req = request({}, {{blob("layer", bytes.size(), {{0, bytes}})}});
    req.reference.kind = ReferenceKind::Stream;
    for (auto size : {uint64_t(0), uint64_t(513), (uint64_t(64) << 40) + 512}) {
        auto file = stream(size, {});
        req.reference.stream = file.get();
        EXPECT_EQ(verify(req).error, Error::ReferenceOpen);
    }
    auto bad = stream(bytes.size(), bytes);
    fputc('X', bad.get());
    rewind(bad.get());
    req.reference.stream = bad.get();
    EXPECT_EQ(verify(req).error, Error::ReferenceOpen);
    auto short_header = stream(bytes.size(), {});
    ASSERT_EQ(ftruncate(fileno(short_header.get()), 7), 0);
    req.reference.stream = short_header.get();
    EXPECT_EQ(verify(req).error, Error::ReferenceOpen);
    bytes.pop_back();
    auto truncated = stream(MIB + 512, bytes);
    req.reference.stream = truncated.get();
    auto result = verify(req);
    EXPECT_EQ(result.error, Error::ReferenceRead);
    EXPECT_EQ(result.offset, MIB);
    bytes.push_back(0x34);
    auto extra = stream(bytes.size(), bytes, true);
    req.reference.stream = extra.get();
    result = verify(req);
    EXPECT_EQ(result.error, Error::ReferenceEnd);
    EXPECT_EQ(result.offset, bytes.size());
    EXPECT_EQ(result.compared_bytes, bytes.size());
}

#ifdef IMAGE_VERIFIER_QEMU_IMG
TEST_F(VerifierTest, LibvhdiReadsIndependentFixedAndDynamicFixtures) {
    std::vector<uint8_t> bytes(MIB + 512, 0x28);
    auto input = raw("input.raw", bytes);
    auto layer = blob("layer", bytes.size(), {{0, bytes}});
    for (const auto &format : {std::string("fixed"), std::string("dynamic")}) {
        auto vhdx = path(format + ".vhdx");
        auto options = "subformat=" + format + ",block_size=1048576";
        auto child = fork();
        ASSERT_GE(child, 0);
        if (!child) {
            execl(IMAGE_VERIFIER_QEMU_IMG, "qemu-img", "convert", "-f", "raw", "-O", "vhdx",
                  "-o", options.c_str(), input.c_str(), vhdx.c_str(), nullptr);
            _exit(127);
        }
        int status = 0;
        ASSERT_EQ(waitpid(child, &status, 0), child);
        ASSERT_TRUE(WIFEXITED(status));
        ASSERT_EQ(WEXITSTATUS(status), 0);
        auto req = request(vhdx, {{layer}});
        req.reference.kind = ReferenceKind::VhdxChain;
        auto result = verify(req);
        EXPECT_TRUE(result.success()) << describe_error(result);
        EXPECT_EQ(result.compared_bytes, bytes.size());
        EXPECT_NE(result.reference_description.find("libvhdi"), std::string::npos);
    }
}
#endif

}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    set_log_output_level(ALOG_ERROR);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_DEFAULT))
        return 1;
    int result = RUN_ALL_TESTS();
    photon::fini();
    return result;
}
