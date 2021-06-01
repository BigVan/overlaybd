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
#include <fcntl.h>
#include <mutex>
#include "overlaybd/alog-audit.h"
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/alog.h"
#include "overlaybd/fs/localfs.h"
#include "overlaybd/fs/tar_file.h"
#include "switch_file.h"
#include "overlaybd/fs/zfile/zfile.h"
#include "overlaybd/photon/thread11.h"
#include "overlaybd/fs/throttled-file.h"
#include "bk_download.h"

using namespace std;

namespace FileSystem {

#define FORWARD(func)                                                                              \
    check_switch();                                                                                \
    ++io_count;                                                                                    \
    DEFER({ --io_count; });                                                                        \
    return m_file->func;

class SwitchFile : public ISwitchFile {
public:
    int io_count;
    bool local_path;
    IFile *m_file = nullptr;
    IFile *m_old = nullptr;
    int state; /* 0. normal state; 1. ready to switch; 2. in processing */
    bool running = false;
    std::string filepath;
    photon::join_handle *dl_thread_jh = nullptr;

    SwitchFile(IFile *source, bool local=false, const char* path=nullptr)
        : m_file(source), local_path(local) {
        state = 0;
        io_count = 0;
        if (path != nullptr)
            filepath = path;
        running = true;
    };

    virtual ~SwitchFile() override {
        running = false;
        if (dl_thread_jh != nullptr) {
            photon::thread_shutdown((photon::thread*) dl_thread_jh);
            photon::thread_join(dl_thread_jh);
        }

        if (m_file != nullptr) {
            safe_delete(m_file);
        }
        if (m_old != nullptr) {
            safe_delete(m_old);
        }
    }

    int do_switch() {
        int flags = O_RDONLY;
        // TODO support libaio
        auto file = open_localfile_adaptor(filepath.c_str(), flags, 0644, 0);
        if (file == nullptr) {
            LOG_ERROR_RETURN(0, -1, "failed to open commit file, path: `, error: `(`)", filepath,
                             errno, strerror(errno));
        }

        // if tar file, open tar file
        file = FileSystem::new_tar_file_adaptor(file);
        //open zfile
        auto zf = ZFile::zfile_open_ro(file, false, true);
        if (!zf) {
            delete file;
            LOG_ERROR_RETURN(0, -1, "zfile_open_ro failed, path: `: error: `(`)", filepath, errno,
                                    strerror(errno));
        }
        file = zf;

        LOG_INFO("switch to localfile '`' success.", filepath);
        m_old = m_file;
        m_file = file;
        local_path = true;
        return 0;
    }

    int check_switch() {
        if (state == 0) {
            return 0;
        }
        if (state == 2) {
            while (state != 0) {
                photon::thread_usleep(1000);
            }
            return 0;
        }
        // state == 1
        state = 2;
        while (io_count > 0) {
            photon::thread_usleep(1000);
        }
        // set set to 0, even do_switch failed
        if (do_switch() == 0) {
            state = 0;
        }
        return 0;
    }

    virtual int close() override {
        FORWARD(close());
    }
    virtual ssize_t read(void *buf, size_t count) override {
        FORWARD(read(buf, count));
    }
    virtual ssize_t readv(const struct iovec *iov, int iovcnt) override {
        FORWARD(readv(iov, iovcnt));
    }
    virtual ssize_t write(const void *buf, size_t count) override {
        FORWARD(write(buf, count));
    }
    virtual ssize_t writev(const struct iovec *iov, int iovcnt) override {
        FORWARD(writev(iov, iovcnt));
    }
    virtual ssize_t pread(void *buf, size_t count, off_t offset) override {
        if (local_path) {
            SCOPE_AUDIT_THRESHOLD(10UL * 1000, "file:pread", AU_FILEOP(filepath, offset, count));
            FORWARD(pread(buf, count, offset));
        } else {
            FORWARD(pread(buf, count, offset));
        }
    }
    virtual ssize_t pwrite(const void *buf, size_t count, off_t offset) override {
        FORWARD(pwrite(buf, count, offset));
    }
    virtual ssize_t preadv(const struct iovec *iov, int iovcnt, off_t offset) override {
        FORWARD(preadv(iov, iovcnt, offset));
    }
    virtual ssize_t pwritev(const struct iovec *iov, int iovcnt, off_t offset) override {
        FORWARD(pwritev(iov, iovcnt, offset));
    }
    virtual off_t lseek(off_t offset, int whence) override {
        FORWARD(lseek(offset, whence));
    }
    virtual int fstat(struct stat *buf) override {
        FORWARD(fstat(buf));
    }
    virtual IFileSystem *filesystem() override {
        FORWARD(filesystem());
    }
    virtual int fsync() override {
        FORWARD(fsync());
    }
    virtual int fdatasync() override {
        FORWARD(fdatasync());
    }
    virtual int sync_file_range(off_t offset, off_t nbytes, unsigned int flags) override {
        FORWARD(sync_file_range(offset, nbytes, flags));
    }
    virtual int fchmod(mode_t mode) override {
        FORWARD(fchmod(mode));
    }
    virtual int fchown(uid_t owner, gid_t group) override {
        FORWARD(fchown(owner, group));
    }
    virtual int ftruncate(off_t length) override {
        FORWARD(ftruncate(length));
    }
    virtual int fallocate(int mode, off_t offset, off_t len) override {
        FORWARD(fallocate(mode, offset, len));
    }


    void download(IFile *src_file, std::string digest, int delay_sec, int max_MB_ps, int max_try) {
        if (BKDL::download_blob(src_file, digest, filepath, delay_sec, max_MB_ps, max_try, running)) {
            // set switch
            state = 1;
        }
        delete src_file;
    }

    void start_download(IFile *src_file, const std::string &digest, int delay_sec, int max_MB_ps, int max_try) {
        dl_thread_jh = photon::thread_enable_join(
            photon::thread_create11(&SwitchFile::download, this, src_file, digest, delay_sec, max_MB_ps, max_try)
        );
    }
};

ISwitchFile *new_switch_file(IFile *file, bool local, const char* filepath) {
    // if tar file, open tar file
    file = FileSystem::new_tar_file_adaptor(file);
    // open zfile
    auto zf = ZFile::zfile_open_ro(file, local ? false : true, true);
    if (!zf) {
        LOG_ERROR_RETURN(0, nullptr, "zfile_open_ro failed, error: `(`)", errno,
                                    strerror(errno));
    }
    file = zf;
    return new SwitchFile(file, local, filepath);
}

ISwitchFile *new_switch_file_with_download(IFile *file, IFile *download_src, const std::string &digest,
                        const char* filepath, int download_delay, int extra, int max_MB_ps, int max_try) {
    SwitchFile *ret = (SwitchFile*) new_switch_file(file, false, filepath);
    if (ret == nullptr)
        return nullptr;
    extra = (extra < 0) ? 30 : extra;
    int delay_sec = (rand() % extra) + download_delay;
    ret->start_download(download_src, digest, delay_sec, max_MB_ps, max_try);
    return ret;
}

} // namespace FileSystem
