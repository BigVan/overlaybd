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
#include <list>
#include <string>
#include "overlaybd/fs/filesystem.h"
#include "switch_file.h"

namespace BKDL {

bool check_downloaded(const std::string &path);

bool download_blob(FileSystem::IFile *source_file, std::string &digest, std::string &dst_file, int delay, int max_MB_ps, int max_try, bool &running);

} // namespace BKDL