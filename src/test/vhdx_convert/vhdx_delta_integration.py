import argparse
from pathlib import Path
import shutil
import subprocess
import tempfile
import time
from vhdx_integration import Validation, fingerprint, require_binaries, write_json


def compare_stream(validation, reader, chain, manifest):
    command = ["mono", str(reader.resolve())] + [str(path) for path in chain]
    started = time.monotonic()
    log = validation.directory / "reference-reader.log"
    with log.open("wb") as output:
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=output)
        try:
            result = validation.run([validation.tool("verify_vhdx_layers"), manifest], stdin=process.stdout)
        finally:
            process.stdout.close()
            returncode = process.wait()
            validation.report["reference_reader"] = {"argv": command, "returncode": returncode,
                                                       "seconds": time.monotonic() - started,
                                                       "log": log.name, "version": log.read_text(errors="replace")}
            if returncode:
                validation.report["status"] = "failed"
            write_json(validation.directory / "report.json", validation.report)
    if returncode:
        raise RuntimeError(f"Independent reference reader failed: {log}")
    return result


def main():
    parser = argparse.ArgumentParser(description="Independent verification of stacked VHDX delta conversions")
    parser.add_argument("--reference-chain", nargs="+", type=Path, required=True)
    parser.add_argument("--lowers", nargs="+", type=Path, required=True)
    parser.add_argument("--zlowers", nargs="+", type=Path, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, default=Path(__file__).resolve().parents[3] / "build/output")
    parser.add_argument("--reference-reader", type=Path,
                        help="DiscUtils-based vhdx_reference.exe (requires Mono); otherwise use libvhdi")
    args = parser.parse_args()
    chain = [path.resolve() for path in args.reference_chain]
    if len(chain) < 2 or len(args.lowers) != len(chain) - 1 or len(args.zlowers) != len(chain) - 1:
        parser.error("provide the root-to-child reference chain and matching converted lower layers")
    args.bin_dir = args.bin_dir.resolve()
    require_binaries(parser, args.bin_dir, ["overlaybd-create", "overlaybd-apply", "overlaybd-commit",
                                          "overlaybd-zfile", "verify_vhdx_layers"])
    for path in chain + args.lowers + args.zlowers:
        if not path.is_file():
            parser.error(f"input file does not exist: {path}")
    if args.reference_reader and not args.reference_reader.is_file():
        parser.error("reference reader executable does not exist")
    if args.reference_reader and not shutil.which("mono"):
        parser.error("mono is required for the DiscUtils reference reader")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    directory = Path(tempfile.mkdtemp(prefix="delta-", dir=args.work_dir.resolve()))
    validation = Validation(directory, args.bin_dir.resolve())
    report = validation.report
    report["reference_chain"] = [str(path) for path in chain]
    print(f"Run directory: {directory}", flush=True)
    print("Hashing reference source files before conversion", flush=True)
    before = [fingerprint(path) for path in chain]
    report["source_before"] = before
    service = directory / "service.json"
    write_json(service, {"logConfig": {"logPath": str(directory / "service.log"), "logLevel": 2},
                         "cacheConfig": {"cacheType": "file", "cacheDir": str(directory / "cache"), "cacheSizeGB": 1},
                         "gzipCacheConfig": {"enable": False}, "enableAudit": False,
                         "download": {"enable": False}, "ioEngine": 0})
    data, index, config = directory / "upper.data", directory / "upper.index", directory / "upper.json"
    write_json(config, {"upper": {"data": str(data), "index": str(index)}, "lowers": [],
                        "download": {"enable": False}})
    validation.run([validation.tool("overlaybd-create"), data, index, "0"])
    validation.run([validation.tool("overlaybd-apply"), "--from_vhdx", "--service_config_path", service, chain[-1], config])
    plain = directory / (chain[-1].stem + ".overlaybd")
    compressed = directory / (chain[-1].stem + ".overlaybd.zfile")
    validation.run([validation.tool("overlaybd-commit"), data, index, plain])
    validation.run([validation.tool("overlaybd-zfile"), plain, compressed])
    groups = [[str(path.resolve()) for path in args.lowers] + [str(plain)],
              [str(path.resolve()) for path in args.zlowers] + [str(compressed)]]
    for name, paths in zip(["stack.json", "stack-zfile.json"], groups):
        write_json(directory / name, {"lowers": [{"file": path} for path in paths],
                                      "download": {"enable": False}})
    manifest = directory / "verification.json"
    if args.reference_reader:
        write_json(manifest, {"reference_stream": "DiscUtils Vhdx (see reference-reader.log)", "layers": groups})
        report["comparison"] = compare_stream(validation, args.reference_reader, chain, manifest)
    else:
        write_json(manifest, {"vhdx": report["reference_chain"], "layers": groups})
        report["comparison"] = validation.run([validation.tool("verify_vhdx_layers"), manifest])
    print("Hashing reference source files after full comparison", flush=True)
    report["source_after"] = [fingerprint(path) for path in chain]
    if before != report["source_after"]:
        report["status"] = "failed"
        write_json(directory / "report.json", report)
        raise RuntimeError("A source VHDX changed during validation")
    report["outputs"] = [{"path": str(path), "bytes": path.stat().st_size,
                           "allocated_bytes": path.stat().st_blocks * 512} for path in [plain, compressed]]
    report["status"] = "passed"
    write_json(directory / "report.json", report)
    for temporary in [data, index, config]:
        temporary.unlink()
    print(f"PASS: artifacts and report in {directory}", flush=True)


if __name__ == "__main__":
    main()
