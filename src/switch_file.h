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
#pragma once

#include "overlaybd/fs/filesystem.h"

namespace FileSystem {

class ISwitchFile: public IFile {
public:
    void start_download(const std::string &digest, int delay_sec, int max_MB_ps, int max_try);
};

// switch to local file after background download finished, and audit for local file pread operations.
// if initialized with local file, only audit for pread.

extern "C" ISwitchFile *new_switch_file(IFile *file, bool local=false, const char* filepath=nullptr);

extern "C" ISwitchFile *new_switch_file_with_download(IFile *file, IFile *source, const std::string &digest, const char* file_path,
                                                        int download_delay, int extra, int max_MB_ps, int max_try);
} // namespace FileSystem
