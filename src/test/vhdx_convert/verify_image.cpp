#include "image_verifier.h"
#include <photon/photon.h>
#include <photon/common/alog.h>

int main(int argc, char **argv) {
    if (argc < 3) {
        fprintf(stderr, "Usage: verify_image reference.raw layer [layer...]\n");
        return 2;
    }
    set_log_output_level(ALOG_ERROR);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_DEFAULT))
        return 1;
    DEFER(photon::fini());
    ImageVerifier::Request request;
    request.reference.paths = {argv[1]};
    for (int index = 2; index < argc; ++index)
        request.groups.push_back({argv[index]});
    auto result = ImageVerifier::verify(request, [](const ImageVerifier::Progress &progress) {
        printf("Compared %llu / %llu bytes for %zu layers\n",
               (unsigned long long)progress.compared_bytes,
               (unsigned long long)progress.virtual_bytes, progress.view_count);
        fflush(stdout);
    });
    if (!result.success()) {
        fprintf(stderr, "%s\n", ImageVerifier::describe_error(result).c_str());
        return 1;
    }
    printf("PASS: %llu logical bytes compared for each of %zu layers, including all holes; %.3f seconds\n",
           (unsigned long long)result.compared_bytes, result.view_count, result.seconds);
    return 0;
}
