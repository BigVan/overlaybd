#include "image_verifier.h"
#include <rapidjson/document.h>
#include <photon/photon.h>
#include <photon/common/alog.h>
#include <fstream>
#include <iterator>

int main(int argc, char **argv) {
    if (argc != 2) {
        fprintf(stderr, "Usage: verify_vhdx_layers manifest.json\n"
                        "Manifest: {\"vhdx\":[root,...,child],\"layers\":[[base,...,delta],...]}\n"
                        "Or replace vhdx with reference_stream: reader description, and pipe VHDXRAW1 data to stdin.\n");
        return 2;
    }
    std::ifstream input(argv[1]);
    std::string content((std::istreambuf_iterator<char>(input)), std::istreambuf_iterator<char>());
    rapidjson::Document manifest;
    manifest.Parse(content.c_str());
    if (manifest.HasParseError() || !manifest.IsObject() ||
        !manifest.HasMember("layers") || !manifest["layers"].IsArray() || manifest["layers"].Empty())
        return 2;
    bool streamed = manifest.HasMember("reference_stream");
    if (streamed ? (!manifest["reference_stream"].IsString() || manifest.HasMember("vhdx"))
                 : (!manifest.HasMember("vhdx") || !manifest["vhdx"].IsArray() || manifest["vhdx"].Empty()))
        return 2;
    ImageVerifier::Request request;
    if (streamed) {
        request.reference.kind = ImageVerifier::ReferenceKind::Stream;
        request.reference.stream = stdin;
        request.reference.label = manifest["reference_stream"].GetString();
    } else {
        request.reference.kind = ImageVerifier::ReferenceKind::VhdxChain;
        for (const auto &path : manifest["vhdx"].GetArray()) {
            if (!path.IsString())
                return 2;
            request.reference.paths.emplace_back(path.GetString());
        }
    }
    for (const auto &group : manifest["layers"].GetArray()) {
        if (!group.IsArray() || group.Empty())
            return 2;
        std::vector<std::string> paths;
        for (const auto &path : group.GetArray()) {
            if (!path.IsString())
                return 2;
            paths.emplace_back(path.GetString());
        }
        request.groups.emplace_back(std::move(paths));
    }
    set_log_output_level(ALOG_ERROR);
    if (photon::init(photon::INIT_EVENT_DEFAULT, photon::INIT_IO_DEFAULT))
        return 1;
    DEFER(photon::fini());
    bool announced = false;
    auto result = ImageVerifier::verify(request, [&](const ImageVerifier::Progress &progress) {
        if (!announced) {
            printf("Independent reference: %s\n", progress.reference_description.c_str());
            announced = true;
        }
        printf("Compared %llu / %llu bytes for every stacked view\n",
               (unsigned long long)progress.compared_bytes, (unsigned long long)progress.virtual_bytes);
        fflush(stdout);
    });
    if (!announced && !result.reference_description.empty())
        printf("Independent reference: %s\n", result.reference_description.c_str());
    if (!result.success()) {
        fprintf(stderr, "%s\n", ImageVerifier::describe_error(result).c_str());
        return 1;
    }
    printf("PASS: %llu logical bytes compared for each of %zu stacked views; %.3f seconds\n",
           (unsigned long long)result.compared_bytes, result.view_count, result.seconds);
    return 0;
}
