#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <sys/types.h>

namespace LSMT {
class IFileRW;
}

namespace VHDX {

class Reader {
public:
    Reader();
    ~Reader();
    Reader(const Reader &) = delete;
    Reader &operator=(const Reader &) = delete;

    int open(const char *path);
    uint64_t virtual_size() const;
    uint32_t logical_sector_size() const;
    bool is_differencing() const;
    ssize_t read_data(uint64_t offset, void *buffer, size_t count) const;

private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
    friend int convert_to_layer(const Reader &reader, LSMT::IFileRW *target, bool verbose);
};

int convert_to_layer(const Reader &reader, LSMT::IFileRW *target, bool verbose = false);

}
