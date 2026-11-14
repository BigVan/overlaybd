# VHDX 转换为 OverlayBD

`overlaybd-apply --from_vhdx` 原生读取固定盘、动态盘或差分 VHDX，将本文件的数据转换为新的 OverlayBD 层。独立盘生成完整磁盘层，差分盘仅生成本层差异。普通只读层通过 `overlaybd-commit` 生成；压缩层通过 `overlaybd-zfile` 或 `overlaybd-commit -z` 生成。转换工具不依赖 QEMU 或 libvhdi。

## 支持范围

- 支持 VHDX v1、512/4096 字节逻辑扇区，保留虚拟容量、分区表及所有逻辑偏移。
- 源文件必须离线、保持不变，并且活动 header 的 LogGuid 为零。需要日志回放的镜像会被拒绝。
- 差分盘转换不查找或打开父 VHDX，不校验父 GUID，不绑定 OverlayBD parent UUID。镜像中记录的父路径不可访问也不影响转换。
- 打开源盘时先完整扫描 BAT 和差分盘使用的 sector bitmap，按逻辑偏移从小到大整理、检查并合并相邻区间，再开始转换。源文件中的物理块顺序不影响 append upper 的写入顺序；继承区间不进入转换索引。索引内存随实际区间数量增长，不按虚拟容量分配数据缓冲区。
- 转换使用 `overlaybd-create` 默认创建的 append upper，要求无 lower、索引为空；不使用 `-s` 或 `--hybrid`。容量由转换器自动设置，源盘虚拟容量须为 4KiB 倍数，不符合时在写入前拒绝。
- 独立盘维持零值稀疏化行为。差分盘中未分配块和 sector bitmap 未置位的扇区不产生映射，叠加时继承 lower；已分配数据中的零、未定义块、显式零块和已取消映射块则生成显式零映射，遮盖 lower。数据和位图各使用固定 1MiB 缓冲区。
- 不支持 rootfs 提取；`--no-extract-rootfs` 可选。不能同时使用 `--from_qcow2`、`--raw`、`--mkfs`、`--checksum` 或 `--gz_index_path`。
- 任何转换错误都会返回非零。不要提交失败的 upper；重试时创建新的空层，不支持断点续传。

## 其他节点从源码构建

在目标 Linux 节点获取包含本功能提交的源码后，从仓库根目录操作。Debian/Ubuntu 的构建依赖示例如下；其他发行版参照仓库 [README](../README.md) 安装相应开发包。

```bash
sudo apt-get update
sudo apt-get install -y build-essential cmake git python3 pkg-config autoconf automake libtool \
  libcurl4-openssl-dev libssl-dev libaio-dev libnl-3-dev libnl-genl-3-dev \
  libgflags-dev libgtest-dev libzstd-dev libext2fs-dev libglib2.0-dev \
  uuid-dev libjson-c-dev libkmod-dev libsystemd-dev libpci-dev nasm help2man

git submodule update --init --recursive
cmake -S . -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=ON -DBUILD_UBLK_FRONTEND=OFF
cmake --build build --target overlaybd-apply overlaybd-create overlaybd-commit overlaybd-zfile \
  verify_image verify_vhdx_layers vhdx_converter_test image_verifier_test -j4
(cd build && ctest -R '^(vhdx_converter_test|image_verifier_test)$' --output-on-failure)
BIN="$PWD/build/output"
```

首次配置会下载仓库声明的构建依赖，需要网络。直接使用本节点生成的 `build/output` 工具，并保留对应构建目录；这些工具可能链接该目录内的定制 libext2fs，共享库路径不能通过单独复制二进制迁移到其他节点。无需安装或启动 TCMU/ublk，也无需先准备 `/etc/overlaybd`。

## 转换示例

以下流程只做原生转换，不需要 QEMU、Mono 或 libvhdi。将 `IMAGE` 替换为目标节点上的源文件，将 `WORKDIR` 设置到有足够空间的目录。可写层和两种输出需要实际磁盘空间，不能仅按压缩后大小估算。

```bash
BIN="$PWD/build/output"
IMAGE=/absolute/path/golden.vhdx
WORKDIR="$PWD/build/vhdx-validation"
mkdir -p "$WORKDIR"
OUT=$(mktemp -d "$WORKDIR/convert-XXXXXX")
SERVICE="$OUT/service.json"
python3 - "$OUT" <<'PY'
import json
from pathlib import Path
import sys
out = Path(sys.argv[1]).resolve()
config = {
    "logConfig": {"logPath": str(out / "service.log"), "logLevel": 2},
    "cacheConfig": {"cacheType": "file", "cacheDir": str(out / "cache"), "cacheSizeGB": 1},
    "gzipCacheConfig": {"enable": False}, "enableAudit": False,
    "download": {"enable": False}, "ioEngine": 0
}
(out / "service.json").write_text(json.dumps(config, indent=2) + "\n")
PY

"$BIN/overlaybd-create" "$OUT/upper.data" "$OUT/upper.index" 0
cat > "$OUT/upper.json" <<EOF
{
  "lowers": [],
  "upper": {"data": "$OUT/upper.data", "index": "$OUT/upper.index"},
  "download": {"enable": false}
}
EOF

"$BIN/overlaybd-apply" --from_vhdx --service_config_path "$SERVICE" \
  "$IMAGE" "$OUT/upper.json" &&
"$BIN/overlaybd-commit" "$OUT/upper.data" "$OUT/upper.index" "$OUT/golden.overlaybd" &&
"$BIN/overlaybd-zfile" "$OUT/golden.overlaybd" "$OUT/golden.overlaybd.zfile"
```

转换沿用默认 4KiB 对齐配置，无需按源盘扇区大小设置 `block4k`。源 VHDX 的逻辑扇区大小仍用于解析 BAT 和差分位图；本层拥有的 512 字节扇区保持精确映射，不将映射扩大到相邻的继承扇区。默认压缩算法为 LZ4、压缩块大小为 4KiB。只需要压缩产物时，最后两条命令可替换为：

```bash
"$BIN/overlaybd-commit" -z "$OUT/upper.data" "$OUT/upper.index" "$OUT/golden.overlaybd.zfile"
```

普通层和压缩层均可直接作为 lower 使用：

```json
{
  "lowers": [{"file": "/absolute/path/golden.overlaybd.zfile"}],
  "download": {"enable": false}
}
```

不需要解压 ZFile，也不需要挂载源 VHDX。确认提交和验证成功后，可以删除本次创建的临时 upper；保留最终产物及其只读配置。

## 差异层与多层组合

转换 `layer1.vhdx` 使用相同流程：新建一个空 upper，将示例中的 `IMAGE` 替换为子盘路径，输出文件名替换为 `layer1.overlaybd` 和 `layer1.overlaybd.zfile`。无需提供 golden 的路径，转换期间的配置仍为 `lowers: []`。

差异层不是完整的独立磁盘。使用时按根层到最新层的顺序组合：

```json
{
  "lowers": [
    {"file": "/absolute/path/golden.overlaybd.zfile"},
    {"file": "/absolute/path/layer1.overlaybd.zfile"},
    {"file": "/absolute/path/layer2.overlaybd.zfile"}
  ],
  "download": {"enable": false}
}
```

普通层与 ZFile 层可以混用。每个后续 VHDX 单独转换后追加到列表末尾即可；各层的块大小可以不同。选择正确的 lower 顺序由调用方负责，转换器不会验证某个 OverlayBD 层是否来自 VHDX 中记录的父盘。

## 验证

完成上述构建后，独立盘的转换与完整内容验证可直接使用脚本。脚本会在输出目录生成自己的服务配置，不依赖系统配置：

```bash
sudo apt-get install -y qemu-utils
BIN="$PWD/build/output"
WORKDIR="$PWD/build/vhdx-validation"

# 先用自动生成的小镜像验证当前节点环境。
python3 src/test/vhdx_convert/vhdx_integration.py --bin-dir "$BIN" --work-dir "$WORKDIR/smoke"

# 验证用户指定的独立 VHDX，完整比较其全部虚拟容量。
IMAGE=/absolute/path/golden.vhdx
python3 src/test/vhdx_convert/vhdx_integration.py --image "$IMAGE" \
  --bin-dir "$BIN" --work-dir "$WORKDIR/full"
```

集成验证需要 `qemu-img`（Ubuntu 的 `qemu-utils` 软件包），不加入默认 CTest。未指定 `--image` 时生成小型固定盘和动态盘，并验证 CLI 错误、两种输出、直接压缩提交和 qcow2 回归。

指定镜像时，每次在工作目录下创建独立的 `run-*` 子目录，生成两种产物和可直接使用的只读配置。QEMU 解码为稀疏 raw 基准，`verify_image` 对整个虚拟空间逐字节比较，包括全部空洞；例如 1000GiB 的虚拟磁盘需要比较全部 1000GiB，而不是仅比较源文件实际占用的空间。还会校验源文件转换前后的 SHA-256、大小和修改时间。

成功后保留产物、配置、逐步日志和 `report.json`，清理本次生成的 raw 基准与临时 upper；失败时保留诊断文件。报告记录命令、QEMU 版本、耗时、比较字节数及产物实际磁盘占用。`verify_image` 按 LSMT 的 `fstat` 容量限定逻辑空间，并检查索引不越过该边界。

两个验证命令共用测试专用的 `image_verifier` 模块。`verify_image reference.raw layer [layer...]` 分别比较每个产物；`verify_vhdx_layers manifest.json` 将每组 lower 按根层到最新层叠加后比较。所有视图先检查容量和索引范围，再以固定 1MiB 缓冲区比较完整空间。失败信息包含参考数据或产物路径、视图编号和可确定的逻辑偏移；内容差异还包含双方的字节值。命令成功返回 0，读取或内容验证失败返回 1，用法错误返回 2。

共享模块只链接到测试程序，接受 raw、libvhdi 或 DiscUtils 参考流，不使用转换器自身的读取逻辑作为基准。`image_verifier_test` 覆盖叠层、显式零、空洞、末块、空 raw、短读、容量和索引越界、差异定位及参考流头部和 EOF；同时安装 libvhdi 和 qemu-img 时追加独立 VHDX 夹具验证。`verify_delta_reconversion` 仍单独检查映射覆盖关系，以区分继承区域和显式清零。

### 差分盘独立验收

QEMU 的 VHDX 差分盘支持不足，因此差分集成验证使用独立的 DiscUtils 读取器。它采用与转换规则一致的 `UNDEFINED/UNMAPPED` 清零语义。测试需要 Mono 的 `mono-mcs` 软件包和官方 DiscUtils 0.16.13 程序集；这些依赖只用于验证，不进入转换工具。

```bash
sudo apt-get install -y mono-mcs curl unzip
BIN="$PWD/build/output"
REF="$PWD/build/discutils-reference"
mkdir -p "$REF"
for component in core streams vhdx; do
  curl -fL "https://api.nuget.org/v3-flatcontainer/discutils.$component/0.16.13/discutils.$component.0.16.13.nupkg" \
    -o "$REF/$component.nupkg"
  unzip -jo "$REF/$component.nupkg" 'lib/net45/*.dll' -d "$REF"
done
mcs -r:"$REF/DiscUtils.Core.dll" -r:"$REF/DiscUtils.Streams.dll" \
  -r:"$REF/DiscUtils.Vhdx.dll" -out:"$REF/vhdx_reference.exe" src/test/vhdx_convert/vhdx_reference.cs

PARENT_VHDX=/absolute/path/golden.vhdx
CHILD_VHDX=/absolute/path/layer1.vhdx
PARENT_LAYER=/absolute/path/golden.overlaybd
PARENT_ZFILE=/absolute/path/golden.overlaybd.zfile
WORKDIR="$PWD/build/vhdx-validation"
python3 src/test/vhdx_convert/vhdx_delta_integration.py \
  --reference-chain "$PARENT_VHDX" "$CHILD_VHDX" \
  --reference-reader "$REF/vhdx_reference.exe" \
  --lowers "$PARENT_LAYER" --zlowers "$PARENT_ZFILE" \
  --bin-dir "$BIN" --work-dir "$WORKDIR/delta"
```

`--reference-chain` 只属于验收脚本，按根盘到子盘提供独立读取器的参考链，不是转换命令的参数。`--lowers` 与 `--zlowers` 分别提供两套已转换的祖先层；测试程序逐字节比较两套最终视图与 DiscUtils 的完整链视图，包括所有继承区间和空洞。参考读取器只读打开源盘，输出容量及全部逻辑字节，比较程序也会拒绝数据流截断或超出声明容量。遇到独立读取器不兼容或内容差异会失败并保留诊断文件，不改用转换器自身作为基准。

安装 `libvhdi-dev` 后重新运行 CMake，也可以在不传 `--reference-reader` 时使用 libvhdi。libvhdi 20210425 对样本部分 `UNDEFINED` 块返回非零数据，严格比较会报告差异；这不能算作零差异验收通过。[VHDX 规范](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-vhdx/01da203b-b3d7-487d-928b-22a460bbe177)允许该状态返回任意数据或零，本实现明确选择零。DiscUtils 的[独立实现](https://github.com/DiscUtils/DiscUtils/blob/master/Library/DiscUtils.Vhdx/ContentStream.cs)采用相同选择，验证时不需要修改读取器或归一化比较结果。

每次在工作目录下新建 `delta-*` 子目录，保留子层两种产物、`stack.json`、`stack-zfile.json`、日志和报告，不覆盖已有父层。完成源文件 SHA-256 复核后清理本次 upper。校验器流式比较，无需生成 1000GiB 的中间 raw。
