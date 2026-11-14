import argparse
import hashlib
import json
from pathlib import Path
import shlex
import shutil
import subprocess
import struct
import tempfile
import time
import uuid


def logical_sector_size(path):
    with path.open("rb") as source:
        source.seek(196608)
        regions = source.read(65536)
        for index in range(min(struct.unpack_from("<I", regions, 8)[0], 2047)):
            entry = 16 + index * 32
            if uuid.UUID(bytes_le=regions[entry:entry + 16]) != uuid.UUID("8b7ca206-4790-4b9a-b8fe-575f050f886e"):
                continue
            location = struct.unpack_from("<Q", regions, entry + 16)[0]
            source.seek(location)
            metadata = source.read(65536)
            for item in range(min(struct.unpack_from("<H", metadata, 10)[0], 2047)):
                offset = 32 + item * 32
                if uuid.UUID(bytes_le=metadata[offset:offset + 16]) == uuid.UUID("8141bf1d-a96f-4709-ba47-f233a8faab5f"):
                    source.seek(location + struct.unpack_from("<I", metadata, offset + 16)[0])
                    sector = struct.unpack("<I", source.read(4))[0]
                    if sector in (512, 4096):
                        return sector
    raise ValueError("Cannot determine the VHDX logical sector size")


def fingerprint(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(4 * 1024 * 1024)
            if not chunk:
                break
            digest.update(chunk)
    status = path.stat()
    return {"sha256": digest.hexdigest(), "size": status.st_size,
            "mtime_ns": status.st_mtime_ns, "ctime_ns": status.st_ctime_ns}


def write_json(path, value):
    path.write_text(json.dumps(value, indent=2) + "\n")


def require_binaries(parser, directory, names):
    for name in names:
        if not shutil.which(str(directory / name)):
            parser.error(f"build the required executable first: {directory / name}")


class Validation:
    def __init__(self, directory, binaries):
        self.directory = directory
        self.binaries = binaries
        self.report = {"commands": [], "status": "running"}

    def run(self, arguments, reject=False, stdin=None):
        arguments = [str(argument) for argument in arguments]
        print(shlex.join(arguments), flush=True)
        started = time.monotonic()
        log = self.directory / f"step-{len(self.report['commands']):02d}.log"
        with log.open("wb") as log_output:
            result = subprocess.run(arguments, stdin=stdin, stdout=log_output, stderr=subprocess.STDOUT)
        output = log.read_text(errors="replace")
        self.report["commands"].append({"argv": arguments, "returncode": result.returncode,
                                        "seconds": time.monotonic() - started, "log": log.name})
        write_json(self.directory / "report.json", self.report)
        if (result.returncode == 0) == reject:
            self.report["status"] = "failed"
            write_json(self.directory / "report.json", self.report)
            raise RuntimeError(f"Unexpected exit status {result.returncode}: {shlex.join(arguments)}\n{output[-4000:]}")
        return output

    def tool(self, name):
        return self.binaries / name

    def validate(self, source, direct_zfile=False, cli_checks=False):
        self.report["source"] = str(source)
        print("Hashing source before conversion", flush=True)
        before = fingerprint(source)
        self.report["source_before"] = before
        self.report["qemu_version"] = self.run(["qemu-img", "--version"])
        info = json.loads(self.run(["qemu-img", "info", "-f", "vhdx", "--output=json", source]))
        self.report["virtual_size"] = info["virtual-size"]
        sector_size = logical_sector_size(source)
        self.report["logical_sector_size"] = sector_size
        service = self.directory / "service.json"
        write_json(service, {"logConfig": {"logLevel": 2, "logPath": str(self.directory / "service.log")}, "enableAudit": False,
                             "cacheConfig": {"cacheType": "file", "cacheDir": str(self.directory / "cache"), "cacheSizeGB": 1},
                             "gzipCacheConfig": {"enable": False}, "download": {"enable": False},
                             "ioEngine": 0, "registryFsVersion": "v2"})
        data, index = self.directory / "upper.data", self.directory / "upper.index"
        config = self.directory / "upper.json"
        write_json(config, {"lowers": [], "upper": {"data": str(data), "index": str(index)},
                            "download": {"enable": False}})
        self.run([self.tool("overlaybd-create"), data, index, "0"])
        apply = [self.tool("overlaybd-apply"), "--from_vhdx", "--service_config_path", service, source, config]
        if cli_checks:
            untouched = [fingerprint(data), fingerprint(index)]
            for flags in [["--from_qcow2"], ["--raw"], ["--mkfs"], ["--extract-rootfs"],
                          ["--checksum", "invalid"], ["--gz_index_path", "unused"]]:
                self.run(apply + flags, reject=True)
                assert untouched == [fingerprint(data), fingerprint(index)], "Argument rejection modified the target"
            invalid_source = self.directory / "invalid.vhdx"
            invalid_source.write_bytes(b"not a virtual disk")
            invalid_apply = [self.tool("overlaybd-apply"), "--from_vhdx", "--service_config_path", service,
                             invalid_source, config]
            self.run(invalid_apply, reject=True)
            assert untouched == [fingerprint(data), fingerprint(index)], "Source rejection modified the target"
            unaligned = self.directory / "unaligned.vhdx"
            self.run(["qemu-img", "create", "-f", "vhdx", unaligned, str(1024 * 1024 + 512)])
            self.run([self.tool("overlaybd-apply"), "--from_vhdx", "--service_config_path", service,
                      unaligned, config], reject=True)
            assert untouched == [fingerprint(data), fingerprint(index)], "Unaligned capacity rejection modified the target"
        self.run(apply + ["--no-extract-rootfs"])
        if cli_checks:
            unchanged = [fingerprint(data), fingerprint(index)]
            rejection = self.run(apply, reject=True)
            assert "target must be an empty writable layer" in rejection, rejection
            assert unchanged == [fingerprint(data), fingerprint(index)], "Nonempty target rejection modified data"
        plain = self.directory / "golden.overlaybd"
        compressed = self.directory / "golden.overlaybd.zfile"
        self.run([self.tool("overlaybd-commit"), data, index, plain])
        self.run([self.tool("overlaybd-zfile"), plain, compressed])
        layers = [plain, compressed]
        if direct_zfile:
            direct = self.directory / "direct.overlaybd.zfile"
            self.run([self.tool("overlaybd-commit"), "-z", data, index, direct])
            layers.append(direct)
        for layer in layers:
            write_json(self.directory / (layer.name + ".json"),
                       {"lowers": [{"file": str(layer)}], "download": {"enable": False}})
        if cli_checks:
            empty_data, empty_index = self.directory / "empty.data", self.directory / "empty.index"
            self.run([self.tool("overlaybd-create"), empty_data, empty_index, "0"])
            lower_config = self.directory / "with-lower.json"
            write_json(lower_config, {"lowers": [{"file": str(plain)}],
                                      "upper": {"data": str(empty_data), "index": str(empty_index)}})
            self.run([self.tool("overlaybd-apply"), "--from_vhdx", "--service_config_path", service,
                      source, lower_config], reject=True)
        reference = self.directory / "reference.raw"
        self.run(["qemu-img", "convert", "-f", "vhdx", "-O", "raw", "-S", "4k", source, reference])
        self.report["comparison"] = self.run([self.tool("verify_image"), reference] + layers)
        if cli_checks:
            qcow = self.directory / "reference.qcow2"
            qcow_data, qcow_index = self.directory / "qcow.data", self.directory / "qcow.index"
            qcow_config = self.directory / "qcow.json"
            qcow_output = self.directory / "qcow.overlaybd"
            qcow_reference = self.directory / "qcow.raw"
            self.run(["qemu-img", "convert", "-f", "raw", "-O", "qcow2", reference, qcow])
            self.run(["qemu-img", "resize", "-f", "qcow2", qcow, "1G"])
            self.run(["qemu-img", "convert", "-f", "qcow2", "-O", "raw", qcow, qcow_reference])
            self.run([self.tool("overlaybd-create"), qcow_data, qcow_index, "1"])
            write_json(qcow_config, {"lowers": [], "upper": {"data": str(qcow_data), "index": str(qcow_index)},
                                    "block4k": False, "download": {"enable": False}})
            self.run([self.tool("overlaybd-apply"), "--from_qcow2", "--no-extract-rootfs",
                      "--service_config_path", service, qcow, qcow_config])
            self.run([self.tool("overlaybd-commit"), qcow_data, qcow_index, qcow_output])
            self.run([self.tool("verify_image"), qcow_reference, qcow_output])
        print("Hashing source after full comparison", flush=True)
        after = fingerprint(source)
        self.report["source_after"] = after
        if before != after:
            raise RuntimeError("Source image changed during validation")
        self.report["outputs"] = [{"path": str(layer), "bytes": layer.stat().st_size,
                                   "allocated_bytes": layer.stat().st_blocks * 512} for layer in layers]
        self.report["reference_allocated_bytes"] = reference.stat().st_blocks * 512
        self.report["status"] = "passed"
        write_json(self.directory / "report.json", self.report)
        for temporary in [reference, data, index]:
            temporary.unlink()
        config.unlink()
        print(f"PASS: artifacts and report in {self.directory}", flush=True)


def main():
    parser = argparse.ArgumentParser(description="Opt-in QEMU-backed, full-disk VHDX conversion validation")
    parser.add_argument("--image", type=Path)
    parser.add_argument("--work-dir", type=Path, required=True)
    parser.add_argument("--bin-dir", type=Path, default=Path(__file__).resolve().parents[3] / "build/output")
    args = parser.parse_args()
    args.bin_dir = args.bin_dir.resolve()
    require_binaries(parser, args.bin_dir, ["overlaybd-create", "overlaybd-apply", "overlaybd-commit",
                                          "overlaybd-zfile", "verify_image"])
    if not shutil.which("qemu-img"):
        parser.error("qemu-img is required only for this integration test")
    if args.image and not args.image.is_file():
        parser.error(f"source VHDX does not exist: {args.image}")
    args.work_dir.mkdir(parents=True, exist_ok=True)
    run_directory = Path(tempfile.mkdtemp(prefix="run-", dir=args.work_dir.resolve()))
    print(f"Run directory: {run_directory}", flush=True)
    if args.image:
        Validation(run_directory, args.bin_dir.resolve()).validate(args.image.resolve())
        return
    raw = run_directory / "input.raw"
    with raw.open("wb") as output:
        output.truncate(17 * 1024 * 1024 + 4096)
        for offset in [0, 1024 * 1024 - 512, 8 * 1024 * 1024, 17 * 1024 * 1024]:
            output.seek(offset)
            output.write(bytes((index % 251 + 1 for index in range(512))))
    for disk_type in ["dynamic", "fixed"]:
        directory = run_directory / disk_type
        directory.mkdir()
        validation = Validation(directory, args.bin_dir.resolve())
        source = directory / "input.vhdx"
        validation.run(["qemu-img", "convert", "-f", "raw", "-O", "vhdx", "-o",
                        f"subformat={disk_type},block_size=1048576", raw, source])
        validation.validate(source, direct_zfile=True, cli_checks=True)


if __name__ == "__main__":
    main()
