#include "image_verifier.h"
#include "../../overlaybd/lsmt/file.h"
#include "../../overlaybd/zfile/zfile.h"
#include <photon/fs/localfs.h>
#ifdef VHDX_TEST_LIBVHDI
#include <libvhdi.h>
#endif
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <memory>
#include <sstream>

namespace ImageVerifier {
namespace {

using File = std::unique_ptr<photon::fs::IFile>;

bool fail(Result &result, Error error, const std::string &message,
          const std::string &path = {}, uint64_t offset = UINT64_MAX, size_t view = SIZE_MAX) {
    result.error = error;
    result.message = message;
    result.path = path;
    result.offset = offset;
    result.view_index = view;
    return false;
}

struct Source {
    const Reference &reference;
    File raw;
#ifdef VHDX_TEST_LIBVHDI
    std::vector<libvhdi_file_t *> chain;
    libvhdi_error_t *error = nullptr;
#endif

    explicit Source(const Reference &value) : reference(value) {}
    ~Source() {
#ifdef VHDX_TEST_LIBVHDI
        while (!chain.empty()) {
            auto file = chain.back();
            chain.pop_back();
            libvhdi_file_close(file, nullptr);
            libvhdi_file_free(&file, nullptr);
        }
        libvhdi_error_free(&error);
#endif
    }

    std::string diagnostic(const std::string &message) const {
#ifdef VHDX_TEST_LIBVHDI
        if (error) {
            char buffer[4096] = {};
            libvhdi_error_backtrace_sprint(error, buffer, sizeof(buffer));
            return message + ": " + buffer;
        }
#endif
        return message;
    }

    std::string path() const {
        return reference.kind == ReferenceKind::Stream ? reference.label : reference.paths.back();
    }

    bool open(Result &result) {
        if (reference.kind == ReferenceKind::RawFile) {
            if (reference.paths.size() != 1)
                return fail(result, Error::InvalidRequest, "Raw reference requires exactly one path");
            raw.reset(photon::fs::open_localfile_adaptor(path().c_str(), O_RDONLY));
            struct stat status;
            if (!raw || raw->fstat(&status) || status.st_size < 0)
                return fail(result, Error::ReferenceOpen, "Cannot open or stat raw reference", path());
            result.virtual_bytes = status.st_size;
        } else if (reference.kind == ReferenceKind::Stream) {
            if (!reference.stream)
                return fail(result, Error::InvalidRequest, "Reference stream is null");
            uint8_t header[16];
            if (fread(header, 1, sizeof(header), reference.stream) != sizeof(header) ||
                memcmp(header, "VHDXRAW1", 8))
                return fail(result, Error::ReferenceOpen, "Invalid reference stream header", path());
            for (size_t index = 0; index < 8; ++index)
                result.virtual_bytes |= uint64_t(header[8 + index]) << (8 * index);
            result.reference_description = reference.label;
        } else if (reference.kind == ReferenceKind::VhdxChain) {
            if (reference.paths.empty())
                return fail(result, Error::InvalidRequest, "VHDX reference chain is empty");
#ifdef VHDX_TEST_LIBVHDI
            for (const auto &name : reference.paths) {
                libvhdi_file_t *file = nullptr;
                if (libvhdi_file_initialize(&file, &error) != 1)
                    return fail(result, Error::ReferenceOpen, diagnostic("Cannot initialize libvhdi"), name);
                chain.push_back(file);
                uint64_t size = 0;
                if (libvhdi_file_open(file, name.c_str(), LIBVHDI_OPEN_READ, &error) != 1 ||
                    libvhdi_file_get_media_size(file, &size, &error) != 1)
                    return fail(result, Error::ReferenceOpen, diagnostic("Cannot open VHDX reference"), name);
                if (chain.size() > 1 && (size != result.virtual_bytes ||
                    libvhdi_file_set_parent_file(file, chain[chain.size() - 2], &error) != 1))
                    return fail(result, Error::ReferenceOpen, diagnostic("Invalid VHDX parent chain or capacity"), name);
                result.virtual_bytes = size;
            }
            result.reference_description = std::string("libvhdi ") + libvhdi_get_version() + ", " +
                                           std::to_string(chain.size()) + " VHDX files";
#else
            return fail(result, Error::ReferenceOpen,
                        "Rebuild with libvhdi-dev, or provide an independent reference stream", path());
#endif
        } else {
            return fail(result, Error::InvalidRequest, "Unknown reference kind");
        }
        if (reference.kind != ReferenceKind::RawFile &&
            (!result.virtual_bytes || result.virtual_bytes > (64ULL << 40) || result.virtual_bytes % 512))
            return fail(result, Error::ReferenceOpen, "Invalid reference virtual size", path());
        return true;
    }

    bool read(void *buffer, size_t count, uint64_t offset, Result &result) {
        ssize_t received = -1;
        if (reference.kind == ReferenceKind::RawFile)
            received = raw->pread(buffer, count, offset);
        else if (reference.kind == ReferenceKind::Stream)
            received = fread(buffer, 1, count, reference.stream);
#ifdef VHDX_TEST_LIBVHDI
        else
            received = libvhdi_file_read_buffer_at_offset(chain.back(), buffer, count, offset, &error);
#endif
        if (received != static_cast<ssize_t>(count))
            return fail(result, Error::ReferenceRead, diagnostic("Reference read failed or was short"), path(), offset);
        return true;
    }

    bool finish(Result &result) {
        if (reference.kind == ReferenceKind::Stream &&
            (fgetc(reference.stream) != EOF || ferror(reference.stream)))
            return fail(result, Error::ReferenceEnd, "Reference stream exceeds virtual size or failed at EOF",
                        path(), result.virtual_bytes);
        return true;
    }
};

struct View {
    std::vector<File> files;
    std::unique_ptr<LSMT::IFileRO> image; // Destroy before its backing files.
};

bool open_view(const std::vector<std::string> &paths, uint64_t size, size_t index,
               View &view, Result &result) {
    std::vector<photon::fs::IFile *> files;
    for (const auto &path : paths) {
        File file(photon::fs::open_localfile_adaptor(path.c_str(), O_RDONLY));
        if (!file)
            return fail(result, Error::LayerOpen, "Cannot open layer", path, UINT64_MAX, index);
        int compressed = ZFile::is_zfile(file.get());
        if (compressed < 0)
            return fail(result, Error::LayerOpen, "Cannot identify layer format", path, UINT64_MAX, index);
        if (compressed) {
            auto decoded = ZFile::zfile_open_ro(file.get(), true, true);
            if (!decoded)
                return fail(result, Error::LayerOpen, "Cannot open ZFile", path, UINT64_MAX, index);
            file.release();
            file.reset(decoded);
        }
        if (!size) {
            // Avoid the stacked opener's unspecified capacity when every layer is empty.
            std::unique_ptr<LSMT::IFileRO> empty(LSMT::open_file_ro(file.get(), false));
            struct stat status;
            if (!empty)
                return fail(result, Error::LayerOpen, "Cannot open LSMT layer", path, UINT64_MAX, index);
            if (empty->fstat(&status) || status.st_size != 0)
                return fail(result, Error::LayerSize, "Size mismatch", path, UINT64_MAX, index);
            if (empty->index()->size())
                return fail(result, Error::LayerRange, "Layer mapping extends beyond virtual size", path, 0, index);
            view.image = std::move(empty);
        }
        files.push_back(file.get());
        view.files.emplace_back(std::move(file));
    }
    if (size)
        view.image.reset(files.size() == 1 ? LSMT::open_file_ro(files.front(), false)
                                          : LSMT::open_files_ro(files.data(), files.size(), false));
    if (!view.image)
        return fail(result, Error::LayerOpen, "Cannot open LSMT view", paths.back(), UINT64_MAX, index);
    struct stat status;
    if (view.image->fstat(&status) || status.st_size < 0 || uint64_t(status.st_size) != size)
        return fail(result, Error::LayerSize, "Size mismatch", paths.back(), UINT64_MAX, index);
    if (view.image->index()->size() && view.image->index()->back().end() > size / 512)
        return fail(result, Error::LayerRange, "Layer mapping extends beyond virtual size", paths.back(),
                    view.image->index()->back().offset * 512, index);
    return true;
}

}

Result verify(const Request &request, const std::function<void(const Progress &)> &progress) {
    auto started = std::chrono::steady_clock::now();
    Result result;
    auto run = [&]() {
        if (request.groups.empty())
            return fail(result, Error::InvalidRequest, "No output views were provided");
        for (const auto &group : request.groups)
            if (group.empty() || group.size() > LSMT::MAX_STACK_LAYERS)
                return fail(result, Error::InvalidRequest, "Invalid number of layers in output view");
        Source source(request.reference);
        if (!source.open(result))
            return false;
        result.view_count = request.groups.size();
        std::vector<std::unique_ptr<View>> views;
        for (size_t index = 0; index < request.groups.size(); ++index) {
            auto view = std::unique_ptr<View>(new View);
            if (!open_view(request.groups[index], result.virtual_bytes, index, *view, result))
                return false;
            views.emplace_back(std::move(view));
        }
        std::vector<uint8_t> expected(1024 * 1024), actual(expected.size());
        uint64_t next_progress = 0;
        for (uint64_t offset = 0; offset < result.virtual_bytes; offset += expected.size()) {
            size_t count = std::min<uint64_t>(expected.size(), result.virtual_bytes - offset);
            if (!source.read(expected.data(), count, offset, result))
                return false;
            for (size_t index = 0; index < views.size(); ++index) {
                if (views[index]->image->pread(actual.data(), count, offset) != static_cast<ssize_t>(count))
                    return fail(result, Error::LayerRead, "Layer read failed or was short",
                                request.groups[index].back(), offset, index);
                if (memcmp(expected.data(), actual.data(), count)) {
                    auto mismatch = std::mismatch(expected.begin(), expected.begin() + count, actual.begin());
                    result.expected = *mismatch.first;
                    result.actual = *mismatch.second;
                    return fail(result, Error::Mismatch, "Mismatch", request.groups[index].back(),
                                offset + (mismatch.first - expected.begin()), index);
                }
            }
            result.compared_bytes = offset + count;
            if (progress && offset >= next_progress) {
                progress({offset, result.virtual_bytes, views.size(), result.reference_description});
                next_progress = offset + (16ULL << 30);
            }
        }
        return source.finish(result);
    };
    run();
    result.seconds = std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count();
    return result;
}

std::string describe_error(const Result &result) {
    std::ostringstream output;
    output << result.message;
    if (result.view_index != SIZE_MAX)
        output << ": view " << result.view_index;
    if (!result.path.empty())
        output << " [" << result.path << "]";
    if (result.offset != UINT64_MAX)
        output << " at byte " << result.offset;
    if (result.error == Error::Mismatch)
        output << " (reference " << unsigned(result.expected) << ", actual " << unsigned(result.actual) << ")";
    return output.str();
}

}
