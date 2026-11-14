#pragma once

#include <cstdint>
#include <cstdio>
#include <functional>
#include <string>
#include <vector>

namespace ImageVerifier {

enum class ReferenceKind { RawFile, VhdxChain, Stream };

struct Reference {
    ReferenceKind kind = ReferenceKind::RawFile;
    std::vector<std::string> paths;
    FILE *stream = nullptr; // Borrowed; the caller closes the stream.
    std::string label;
};

struct Request {
    Reference reference;
    std::vector<std::vector<std::string>> groups; // Each group is ordered root to newest.
};

enum class Error {
    None, InvalidRequest, ReferenceOpen, ReferenceRead, ReferenceEnd,
    LayerOpen, LayerSize, LayerRange, LayerRead, Mismatch
};

struct Progress {
    uint64_t compared_bytes;
    uint64_t virtual_bytes;
    size_t view_count;
    std::string reference_description;
};

struct Result {
    Error error = Error::None;
    std::string message;
    std::string path;
    std::string reference_description;
    size_t view_index = SIZE_MAX;
    uint64_t offset = UINT64_MAX;
    uint64_t virtual_bytes = 0;
    uint64_t compared_bytes = 0; // Prefix compared successfully against every view.
    size_t view_count = 0;
    uint8_t expected = 0;
    uint8_t actual = 0;
    double seconds = 0;

    bool success() const { return error == Error::None; }
};

// Photon must be initialized by the caller. All opened files are read-only.
Result verify(const Request &request, const std::function<void(const Progress &)> &progress = {});
std::string describe_error(const Result &result);

}
