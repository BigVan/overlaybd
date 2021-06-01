/*
   Copyright The Overlaybd Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/
#include <errno.h>
#include <list>
#include <set>
#include <string>
#include <thread>
#include <sys/file.h>
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/alog.h"
#include "overlaybd/fs/localfs.h"
#include "overlaybd/fs/throttled-file.h"
#include "overlaybd/photon/thread.h"
#include "overlaybd/photon/syncio/fd-events.h"
#include "bk_download.h"
#include "overlaybd/event-loop.h"
#include <openssl/sha.h>
#include <sys/stat.h>
#include <unistd.h>
using namespace FileSystem;

static constexpr size_t ALIGNMENT = 4096;

namespace BKDL {

bool downloading = false;

std::string sha256sum(const char* fn) {
    constexpr size_t BUFFERSIZE = 65536;
    int fd = open(fn, O_RDONLY | O_DIRECT);
    if (fd < 0) {
        LOG_ERROR("failed to open `", fn);
        return "";
    }
    DEFER(close(fd););

    struct stat stat;
    if (::fstat(fd, &stat) < 0) {
        LOG_ERROR("failed to stat `", fn);
        return "";
    }
    SHA256_CTX ctx = {0};
    SHA256_Init(&ctx);
    __attribute__((aligned(ALIGNMENT))) char buffer[65536];
    unsigned char sha[32];
    int recv = 0;
    for (off_t offset = 0; offset < stat.st_size; offset += BUFFERSIZE) {
        recv = pread(fd, &buffer, BUFFERSIZE, offset);
        if (recv < 0) {
            LOG_ERROR("io error: `", fn);
            return "";
        }
        if (SHA256_Update(&ctx, buffer, recv) < 0) {
            LOG_ERROR("sha256 calculate error: `", fn);
            return "";
        }
    }
    SHA256_Final(sha, &ctx);
    char res[SHA256_DIGEST_LENGTH * 2];
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
        sprintf(res + (i * 2), "%02x", sha[i]);
    return "sha256:" + std::string(res, SHA256_DIGEST_LENGTH*2);
}

bool check_downloaded(const std::string &path) {
    auto lfs = FileSystem::new_localfs_adaptor();
    if (!lfs) {
        LOG_ERROR("new_localfs_adaptor() return NULL");
        return false;
    }
    DEFER({ delete lfs; });

    if (lfs->access(path.c_str(), 0) == 0)
        return true;
    return false;
}

ssize_t filecopy(IFile *infile, IFile *outfile, size_t bs, int retry_limit, bool &running) {
    if (bs == 0)
        LOG_ERROR_RETURN(EINVAL, -1, "bs should not be 0");
    void *buff = nullptr;

    // buffer allocate, with 4K alignment
    ::posix_memalign(&buff, ALIGNMENT, bs);
    if (buff == nullptr)
        LOG_ERROR_RETURN(ENOMEM, -1, "Fail to allocate buffer with ", VALUE(bs));
    DEFER(free(buff));
    off_t offset = 0;
    ssize_t count = bs;
    while (count == (ssize_t)bs) {
        if (!running) {
            LOG_INFO("file destroyed when background downloading");
            return -1;
        }

        int retry = retry_limit;
    again_read:
        if (!(retry--))
            LOG_ERROR_RETURN(EIO, -1, "Fail to read at ", VALUE(offset), VALUE(count));
        auto rlen = infile->pread(buff, bs, offset);
        if (rlen < 0) {
            LOG_DEBUG("Fail to read at ", VALUE(offset), VALUE(count), " retry...");
            goto again_read;
        }
        retry = retry_limit;
    again_write:
        if (!(retry--))
            LOG_ERROR_RETURN(EIO, -1, "Fail to write at ", VALUE(offset), VALUE(count));
        // cause it might write into file with O_DIRECT
        // keep write length as bs
        auto wlen = outfile->pwrite(buff, bs, offset);
        // but once write lenth larger than read length treats as OK
        if (wlen < rlen) {
            LOG_DEBUG("Fail to write at ", VALUE(offset), VALUE(count), " retry...");
            goto again_write;
        }
        count = rlen;
        offset += count;
    }
    // truncate after write, for O_DIRECT
    outfile->ftruncate(offset);
    return offset;
}

bool download_done(const std::string &digest, std::string &downloaded_file, std::string &dst_file) {
    auto lfs = new_localfs_adaptor();
    if (!lfs) {
        LOG_ERROR("new_localfs_adaptor() return NULL");
        return false;
    }
    DEFER({ delete lfs; });

    // verify sha256
    auto th = photon::CURRENT;
    std::string shares;
    std::thread sha256_thread([&, th](){
        shares = sha256sum(downloaded_file.c_str());
        photon::safe_thread_interrupt(th, EINTR, 0);
    });
    sha256_thread.detach();
    photon::thread_usleep(-1UL);
    if (shares != digest) {
        LOG_ERROR("verify checksum ` failed (expect: `, got: `)", downloaded_file, digest, shares);
        return false;
    }

    int ret = lfs->rename(downloaded_file.c_str(), dst_file.c_str());
    if (ret != 0) {
        LOG_ERROR_RETURN(0, false, "rename(`,`), `:`", downloaded_file, dst_file, errno, strerror(errno));
    }
    LOG_INFO("download done. rename(`,`) success", downloaded_file, dst_file);
    return true;
}

bool download_blob(FileSystem::IFile *source_file, std::string &digest, std::string &dst_file,
                        int delay, int max_MB_ps, int max_try, bool &running) {
    photon::thread_sleep(delay);
    if (!running)
        return false;

    while (downloading) {
        photon::thread_sleep(1);
    }

    downloading = true;
    DEFER(downloading = false;);

    std::string dl_file_path = dst_file + ".download";
    FileSystem::IFile *src = source_file;
    if (max_MB_ps > 0) {
        FileSystem::ThrottleLimits limits;
        limits.R.throughput = max_MB_ps * 1024UL * 1024; // MB
        limits.R.block_size = 1024UL * 1024;
        limits.time_window = 1UL;
        src = FileSystem::new_throttled_file(src, limits);
    }
    DEFER({
        if (max_MB_ps > 0)
             delete src;
    });

    auto dst = FileSystem::open_localfile_adaptor(dl_file_path.c_str(), O_RDWR | O_CREAT, 0644);
    if (dst == nullptr) {
        LOG_ERRNO_RETURN(0, -1, "failed to open dst file `", dl_file_path.c_str());
    }
    DEFER(delete dst;);

    while (max_try-- > 0) {
        auto res = filecopy(src, dst, 1024UL * 1024, 1, running);
        if (res < 0) {
            LOG_WARN("retry download for file `", dst_file);
            continue;
        }
        if (download_done(digest, dl_file_path, dst_file)) {
            return true;
        }
        LOG_WARN("retry download for file `", dst_file);
    }
    return false;
}

} // namespace BKDL