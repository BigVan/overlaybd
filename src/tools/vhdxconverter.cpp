#include "vhdxconverter.h"
#include "../overlaybd/lsmt/file.h"
#include "../overlaybd/zfile/crc32/crc32c.h"
#include <algorithm>
#include <array>
#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <map>
#include <new>
#include <string>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

namespace VHDX {
namespace {

constexpr uint64_t MIB = 1024 * 1024;
constexpr uint64_t MAX_DISK_SIZE = 64ULL * 1024 * 1024 * MIB;
const std::string BAT_GUID("\x66\x77\xc2\x2d\x23\xf6\x00\x42\x9d\x64\x11\x5e\x9b\xfd\x4a\x08", 16);
const std::string META_GUID("\x06\xa2\x7c\x8b\x90\x47\x9a\x4b\xb8\xfe\x57\x5f\x05\x0f\x88\x6e", 16);
const std::array<std::string, 6> ITEM_GUIDS = {{
    std::string("\x37\x67\xa1\xca\x36\xfa\x43\x4d\xb3\xb6\x33\xf0\xaa\x44\xe7\x6b", 16),
    std::string("\x24\x42\xa5\x2f\x1b\xcd\x76\x48\xb2\x11\x5d\xbe\xd8\x3b\xf4\xb8", 16),
    std::string("\x1d\xbf\x41\x81\x6f\xa9\x09\x47\xba\x47\xf2\x33\xa8\xfa\xab\x5f", 16),
    std::string("\xc7\x48\xa3\xcd\x5d\x44\x71\x44\x9c\xc9\xe9\x88\x52\x51\xc5\x56", 16),
    std::string("\xab\x12\xca\xbe\xe6\xb2\x23\x45\x93\xef\xc3\x09\xe0\x00\xc7\x46", 16),
    std::string("\x2d\x5f\xd3\xa8\x0b\xb3\x4d\x45\xab\xf7\xd3\xd8\x48\x34\xab\x0c", 16)
}};

uint64_t little_endian(const uint8_t *bytes, size_t count) {
    uint64_t value = 0;
    for (size_t index = 0; index < count; ++index)
        value |= uint64_t(bytes[index]) << (8 * index);
    return value;
}

int fail(const char *message, uint64_t offset = 0, int error = EINVAL) {
    fprintf(stderr, "VHDX: %s at offset %llu (%s)\n", message,
            static_cast<unsigned long long>(offset), strerror(error));
    errno = error;
    return -1;
}

bool fits(uint64_t offset, uint64_t length, uint64_t limit) {
    return offset <= limit && length <= limit - offset;
}

bool zero(const uint8_t *bytes, size_t count) {
    return std::all_of(bytes, bytes + count, [](uint8_t value) { return value == 0; });
}

bool checksum_valid(std::vector<uint8_t> &bytes, const char *signature) {
    if (memcmp(bytes.data(), signature, 4) != 0)
        return false;
    auto stored = little_endian(bytes.data() + 4, 4);
    memset(bytes.data() + 4, 0, 4);
    auto computed = ~crc32::crc32c_extend(bytes.data(), bytes.size(), UINT32_MAX);
    for (size_t index = 0; index < 4; ++index)
        bytes[4 + index] = stored >> (index * 8);
    return computed == stored;
}

struct Range {
    uint64_t offset;
    uint64_t length;
};

bool disjoint(std::vector<Range> ranges) {
    std::sort(ranges.begin(), ranges.end(), [](const Range &left, const Range &right) {
        return left.offset < right.offset;
    });
    for (size_t index = 1; index < ranges.size(); ++index)
        if (ranges[index].offset - ranges[index - 1].offset < ranges[index - 1].length)
            return false;
    return true;
}

}

struct Reader::Impl {
    struct Extent {
        uint64_t logical;
        uint64_t physical;
        uint64_t length;
        uint64_t bitmap = 0;
        uint64_t bitmap_bit = 0;
    };
    int descriptor = -1;
    uint64_t file_size = 0;
    uint64_t disk_size = 0;
    uint32_t sector_size = 0;
    uint32_t block_size = 0;
    bool differencing = false;
    std::vector<Extent> extents;

    void add_extent(const Extent &extent) {
        if (!extents.empty()) {
            auto &last = extents.back();
            bool contiguous = last.physical ? extent.physical == last.physical + last.length
                                            : extent.physical == 0;
            if (last.logical + last.length == extent.logical && contiguous &&
                last.bitmap == extent.bitmap &&
                (!last.bitmap || last.bitmap_bit + last.length / sector_size == extent.bitmap_bit)) {
                last.length += extent.length;
                return;
            }
        }
        extents.push_back(extent);
    }

    ~Impl() {
        if (descriptor >= 0)
            ::close(descriptor);
    }

    int read_exact(uint64_t offset, void *buffer, size_t count) const {
        if (!fits(offset, count, file_size))
            return fail("read outside source file", offset);
        ssize_t result;
        do {
            result = ::pread(descriptor, buffer, count, offset);
        } while (result < 0 && errno == EINTR);
        if (result != static_cast<ssize_t>(count))
            return fail("source read failed or was short", offset, result < 0 ? errno : EIO);
        return 0;
    }

    int parse() {
        uint8_t identifier[8];
        if (read_exact(0, identifier, sizeof(identifier)) != 0)
            return -1;
        if (memcmp(identifier, "vhdxfile", 8) != 0)
            return fail("invalid file identifier");

        std::array<std::vector<uint8_t>, 2> headers = {{
            std::vector<uint8_t>(4096), std::vector<uint8_t>(4096)}};
        bool valid[2];
        for (size_t index = 0; index < 2; ++index) {
            valid[index] = read_exact((index + 1) * 65536, headers[index].data(), 4096) == 0 &&
                           checksum_valid(headers[index], "head");
        }
        if (!valid[0] && !valid[1])
            return fail("no valid header");
        size_t active = valid[0] ? 0 : 1;
        if (valid[0] && valid[1]) {
            auto first = little_endian(headers[0].data() + 8, 8);
            auto second = little_endian(headers[1].data() + 8, 8);
            if (first == second)
                return fail("header sequence numbers are equal");
            active = second > first ? 1 : 0;
        }
        const auto &header = headers[active];
        if (little_endian(header.data() + 66, 2) != 1)
            return fail("unsupported format version", 0, ENOTSUP);
        if (!zero(header.data() + 48, 16))
            return fail("log replay required; close or repair the source first", 0, ENOTSUP);
        Range log{little_endian(header.data() + 72, 8), little_endian(header.data() + 68, 4)};
        if (log.offset < MIB || log.offset % MIB || log.length < MIB ||
            log.length % MIB || !fits(log.offset, log.length, file_size))
            return fail("invalid log range", log.offset);
        std::vector<Range> physical_ranges{{0, MIB}, log};

        std::array<std::vector<uint8_t>, 2> tables = {{
            std::vector<uint8_t>(65536), std::vector<uint8_t>(65536)}};
        for (size_t index = 0; index < 2; ++index) {
            valid[index] = read_exact((index + 3) * 65536, tables[index].data(), 65536) == 0 &&
                           checksum_valid(tables[index], "regi");
        }
        if (!valid[0] && !valid[1])
            return fail("no valid region table");
        if (valid[0] && valid[1] && tables[0] != tables[1])
            return fail("conflicting region tables");
        const auto &table = tables[valid[0] ? 0 : 1];
        auto region_count = little_endian(table.data() + 8, 4);
        if (region_count > 2047)
            return fail("too many region entries");
        std::map<std::string, Range> regions;
        for (size_t index = 0; index < region_count; ++index) {
            auto entry = table.data() + 16 + 32 * index;
            std::string key(reinterpret_cast<const char *>(entry), 16);
            Range range{little_endian(entry + 16, 8), little_endian(entry + 24, 4)};
            if (range.offset % MIB || !range.length || range.length % MIB ||
                !fits(range.offset, range.length, file_size) || !regions.emplace(key, range).second)
                return fail("invalid or duplicate region", range.offset);
            if ((little_endian(entry + 28, 4) & 1) && key != BAT_GUID && key != META_GUID)
                return fail("unknown required region", range.offset, ENOTSUP);
            physical_ranges.push_back(range);
        }
        if (!regions.count(BAT_GUID) || !regions.count(META_GUID) || !disjoint(physical_ranges))
            return fail("missing or overlapping regions");

        auto metadata = regions.at(META_GUID);
        std::vector<uint8_t> entries(65536);
        if (read_exact(metadata.offset, entries.data(), entries.size()) != 0)
            return -1;
        if (memcmp(entries.data(), "metadata", 8) != 0)
            return fail("invalid metadata signature", metadata.offset);
        auto item_count = little_endian(entries.data() + 10, 2);
        if (item_count > 2047)
            return fail("too many metadata entries");
        std::map<std::string, Range> items;
        std::vector<Range> item_ranges{{0, 65536}};
        for (size_t index = 0; index < item_count; ++index) {
            auto entry = entries.data() + 32 + 32 * index;
            std::string key(reinterpret_cast<const char *>(entry), 16);
            Range range{little_endian(entry + 16, 4), little_endian(entry + 20, 4)};
            auto flags = little_endian(entry + 24, 4);
            if (!fits(range.offset, range.length, metadata.length) ||
                !items.emplace(key, range).second)
                return fail("invalid or duplicate metadata", metadata.offset + range.offset);
            if ((flags & 4) && std::find(ITEM_GUIDS.begin(), ITEM_GUIDS.end(), key) == ITEM_GUIDS.end())
                return fail("unknown required metadata", metadata.offset + range.offset, ENOTSUP);
            if (range.length)
                item_ranges.push_back(range);
        }
        if (!disjoint(item_ranges))
            return fail("overlapping metadata items");
        std::array<std::array<uint8_t, 16>, 5> values{};
        const size_t lengths[] = {8, 8, 4, 4, 16};
        for (size_t index = 0; index < values.size(); ++index) {
            auto found = items.find(ITEM_GUIDS[index]);
            if (found == items.end() || found->second.length != lengths[index])
                return fail("missing or incorrectly sized required metadata");
            if (read_exact(metadata.offset + found->second.offset, values[index].data(), lengths[index]) != 0)
                return -1;
        }
        block_size = little_endian(values[0].data(), 4);
        auto parameters = little_endian(values[0].data() + 4, 4);
        differencing = parameters & 2;
        auto parent = items.find(ITEM_GUIDS[5]);
        if (differencing != (parent != items.end()))
            return fail("parent locator does not match the disk type");
        if (differencing) {
            if (parent->second.length < 20)
                return fail("truncated parent locator");
            std::vector<uint8_t> locator(parent->second.length);
            if (read_exact(metadata.offset + parent->second.offset, locator.data(), locator.size()))
                return -1;
            auto count = little_endian(locator.data() + 18, 2);
            if (!fits(20, count * 12, locator.size()))
                return fail("truncated parent locator entries");
            for (size_t index = 0; index < count; ++index) {
                auto entry = locator.data() + 20 + index * 12;
                for (size_t field = 0; field < 2; ++field) {
                    auto offset = little_endian(entry + field * 4, 4);
                    auto length = little_endian(entry + 8 + field * 2, 2);
                    if (offset < 20 + count * 12 || length % 2 ||
                        !fits(offset, length, locator.size()))
                        return fail("invalid parent locator string range");
                }
            }
        }
        disk_size = little_endian(values[1].data(), 8);
        sector_size = little_endian(values[2].data(), 4);
        auto physical_sector = little_endian(values[3].data(), 4);
        if ((sector_size != 512 && sector_size != 4096) ||
            (physical_sector != 512 && physical_sector != 4096) ||
            block_size < MIB || block_size > 256 * MIB || (block_size & (block_size - 1)) ||
            !disk_size || disk_size > MAX_DISK_SIZE || disk_size % sector_size)
            return fail("invalid disk, block or sector size");

        auto bat = regions.at(BAT_GUID);
        uint64_t blocks = (disk_size + block_size - 1) / block_size;
        uint64_t chunk_ratio = (uint64_t(1) << 23) * sector_size / block_size;
        uint64_t chunks = (blocks + chunk_ratio - 1) / chunk_ratio;
        uint64_t bat_entries = differencing ? chunks * (chunk_ratio + 1)
                                           : blocks + (blocks - 1) / chunk_ratio;
        if (bat_entries > bat.length / 8)
            return fail("BAT is too short", bat.offset);
        std::array<uint8_t, 65536> bat_buffer;
        uint64_t cached_start = UINT64_MAX;
        auto read_bat = [&](uint64_t bat_index, uint64_t &entry) {
            uint64_t page = bat_index * 8 / bat_buffer.size() * bat_buffer.size();
            if (page != cached_start) {
                size_t length = std::min<uint64_t>(bat_buffer.size(), bat.length - page);
                if (read_exact(bat.offset + page, bat_buffer.data(), length) != 0)
                    return -1;
                cached_start = page;
            }
            entry = little_endian(bat_buffer.data() + bat_index * 8 - page, 8);
            return 0;
        };
        std::vector<uint64_t> bitmaps(differencing ? chunks : 0, 0);
        for (uint64_t chunk = 0; chunk < bitmaps.size(); ++chunk) {
            uint64_t entry;
            if (read_bat(chunk * (chunk_ratio + 1) + chunk_ratio, entry))
                return -1;
            auto state = entry & 7;
            if (state != 0 && state != 6)
                return fail("invalid sector bitmap BAT state");
            if (state == 6) {
                auto physical = entry & 0xfffffffffff00000ULL;
                if (!physical || !fits(physical, MIB, file_size))
                    return fail("sector bitmap outside source file", physical);
                bitmaps[chunk] = physical;
                physical_ranges.push_back({physical, MIB});
            }
        }
        for (uint64_t block = 0; block < blocks; ++block) {
            uint64_t bat_index = block + block / chunk_ratio;
            uint64_t entry;
            if (read_bat(bat_index, entry))
                return -1;
            auto state = entry & 7;
            if (state == 4 || state == 5 || (state == 7 && !differencing))
                return fail("invalid payload BAT state", bat.offset + bat_index * 8);
            uint64_t logical = block * block_size;
            uint64_t length = std::min<uint64_t>(block_size, disk_size - logical);
            if (state != 6 && state != 7) {
                if (differencing && state != 0)
                    add_extent({logical, 0, length});
                continue;
            }
            uint64_t physical = entry & 0xfffffffffff00000ULL;
            if (!physical || !fits(physical, length, file_size))
                return fail("payload outside source file", physical);
            if (state == 7 && !bitmaps[block / chunk_ratio])
                return fail("partial payload has no sector bitmap", logical);
            add_extent({logical, physical, length,
                        state == 7 ? bitmaps[block / chunk_ratio] : 0,
                        (block % chunk_ratio) * (block_size / sector_size)});
            physical_ranges.push_back({physical, std::min<uint64_t>(block_size, file_size - physical)});
        }
        if (!disjoint(physical_ranges))
            return fail("overlapping payload or metadata ranges");
        return prepare_index();
    }

    int prepare_index() {
        // Finish scanning the complete BAT and all referenced sector bitmaps before
        // the target is opened. The resulting index contains only owned data/zero
        // ranges, sorted by logical offset regardless of the source's physical layout.
        std::vector<Extent> blocks;
        blocks.swap(extents);
        std::sort(blocks.begin(), blocks.end(), [](const Extent &left, const Extent &right) {
            return left.logical < right.logical;
        });
        std::vector<uint8_t> bitmap(MIB);
        uint64_t cached_bitmap = 0;
        uint64_t logical_end = 0;
        for (const auto &extent : blocks) {
            if (!extent.length || extent.logical < logical_end ||
                !fits(extent.logical, extent.length, disk_size))
                return fail("overlapping or out-of-range logical extents", extent.logical);
            logical_end = extent.logical + extent.length;
            if (!extent.bitmap) {
                extents.push_back({extent.logical, extent.physical, extent.length});
                continue;
            }
            if (cached_bitmap != extent.bitmap) {
                if (read_exact(extent.bitmap, bitmap.data(), bitmap.size()))
                    return -1;
                cached_bitmap = extent.bitmap;
            }
            auto present = [&](uint64_t sector) {
                auto bit = extent.bitmap_bit + sector;
                return (bitmap[bit / 8] >> (bit % 8)) & 1;
            };
            uint64_t sectors = extent.length / sector_size;
            for (uint64_t sector = 0; sector < sectors;) {
                if (!present(sector)) {
                    ++sector;
                    continue;
                }
                auto start = sector++;
                while (sector < sectors && present(sector))
                    ++sector;
                // Preserve ownership-run boundaries so payload zero detection uses
                // the same chunks as conversion did before bitmap pre-scanning.
                extents.push_back({extent.logical + start * sector_size,
                                   extent.physical + start * sector_size, (sector - start) * sector_size});
            }
        }
        return 0;
    }
};

Reader::Reader() = default;
Reader::~Reader() = default;

int Reader::open(const char *path) {
    impl_.reset();
    try {
        auto candidate = std::unique_ptr<Impl>(new Impl);
        candidate->descriptor = ::open(path, O_RDONLY | O_CLOEXEC);
        if (candidate->descriptor < 0)
            return fail("cannot open source", 0, errno);
        struct stat status;
        if (::fstat(candidate->descriptor, &status) != 0)
            return fail("cannot stat source", 0, errno);
        if (!S_ISREG(status.st_mode) || status.st_size < 0)
            return fail("source must be a regular file");
        candidate->file_size = status.st_size;
        if (candidate->parse() != 0)
            return -1;
        impl_ = std::move(candidate);
        return 0;
    } catch (const std::bad_alloc &) {
        return fail("metadata allocation failed", 0, ENOMEM);
    }
}

uint64_t Reader::virtual_size() const {
    return impl_ ? impl_->disk_size : 0;
}

uint32_t Reader::logical_sector_size() const {
    return impl_ ? impl_->sector_size : 0;
}

bool Reader::is_differencing() const {
    return impl_ && impl_->differencing;
}

ssize_t Reader::read_data(uint64_t offset, void *buffer, size_t count) const {
    if (!impl_)
        return fail("reader is not open", offset, EBADF);
    if (impl_->differencing)
        return fail("a delta has no standalone disk view; convert and stack its layer", offset, ENOTSUP);
    if (offset >= impl_->disk_size)
        return 0;
    count = std::min<uint64_t>(count, impl_->disk_size - offset);
    memset(buffer, 0, count);
    auto found = std::lower_bound(impl_->extents.begin(), impl_->extents.end(), offset,
        [](const Impl::Extent &extent, uint64_t position) {
            return extent.logical + extent.length <= position;
        });
    for (; found != impl_->extents.end() && found->logical < offset + count; ++found) {
        auto start = std::max(offset, found->logical);
        auto end = std::min<uint64_t>(offset + count, found->logical + found->length);
        if (impl_->read_exact(found->physical + start - found->logical,
                             static_cast<uint8_t *>(buffer) + start - offset, end - start) != 0)
            return -1;
    }
    return count;
}

int convert_to_layer(const Reader &reader, LSMT::IFileRW *target, bool verbose) {
    if (!reader.impl_ || !target)
        return fail("source or target is not open");
    if (target->get_lower_files().size() != 1 || target->index()->size() != 0)
        return fail("target must be an empty writable layer without lowers");
    if (reader.virtual_size() % LSMT::ALIGNMENT4K)
        return fail("virtual size must be 4K aligned");
    void *allocation = nullptr;
    int allocation_error = posix_memalign(&allocation, 4096, MIB);
    if (allocation_error != 0)
        return fail("conversion buffer allocation failed", 0, allocation_error);
    std::unique_ptr<uint8_t, decltype(&free)> buffer(static_cast<uint8_t *>(allocation), &free);
    if (target->update_vsize(reader.virtual_size()) != 0)
        return fail("cannot set target virtual size", 0, errno);
    uint64_t written = 0;
    uint64_t zeroed = 0;
    uint64_t zero_start = 0, zero_length = 0;
    auto flush_zeros = [&]() {
        uint64_t max_length = uint64_t(LSMT::Segment::MAX_LENGTH) * LSMT::ALIGNMENT;
        max_length -= max_length % reader.logical_sector_size();
        while (zero_length) {
            auto length = std::min(zero_length, max_length);
            if (target->fallocate(FALLOC_FL_ZERO_RANGE | FALLOC_FL_KEEP_SIZE, zero_start, length) != 0)
                return fail("target zero mapping failed", zero_start, errno);
            zero_start += length;
            zero_length -= length;
        }
        return 0;
    };
    auto queue_zeros = [&](uint64_t logical, uint64_t length) {
        if (!reader.is_differencing())
            return 0;
        if (zero_length && zero_start + zero_length != logical && flush_zeros())
            return -1;
        if (!zero_length)
            zero_start = logical;
        zero_length += length;
        zeroed += length;
        return 0;
    };
    auto convert_range = [&](uint64_t logical, uint64_t physical, uint64_t count) {
        if (!physical)
            return queue_zeros(logical, count);
        for (uint64_t position = 0; position < count;) {
            size_t length = std::min<uint64_t>(MIB, count - position);
            if (physical && reader.impl_->read_exact(physical + position, buffer.get(), length) != 0) {
                fprintf(stderr, "VHDX: payload read failed at logical offset %llu\n",
                        static_cast<unsigned long long>(logical + position));
                return -1;
            }
            if (zero(buffer.get(), length)) {
                if (queue_zeros(logical + position, length))
                    return -1;
            } else {
                if (flush_zeros())
                    return -1;
                auto result = target->pwrite(buffer.get(), length, logical + position);
                if (result != static_cast<ssize_t>(length))
                    return fail("target write failed or was short", logical + position,
                                result < 0 ? errno : EIO);
                written += length;
            }
            position += length;
        }
        if (verbose)
            fprintf(stderr, "VHDX: converted through logical offset %llu / %llu\n",
                    static_cast<unsigned long long>(logical + count),
                    static_cast<unsigned long long>(reader.virtual_size()));
        return 0;
    };
    for (const auto &extent : reader.impl_->extents) {
        if (convert_range(extent.logical, extent.physical, extent.length))
            return -1;
    }
    if (flush_zeros())
        return -1;
    if (target->fsync() != 0)
        return fail("target sync failed", 0, errno);
    printf("VHDX %s conversion complete: virtual size %llu, written %llu, explicitly zeroed %llu bytes\n",
           reader.is_differencing() ? "delta" : "disk",
           static_cast<unsigned long long>(reader.virtual_size()),
           static_cast<unsigned long long>(written), static_cast<unsigned long long>(zeroed));
    return 0;
}

}
