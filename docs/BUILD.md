# Native builds: macOS, Linux, Windows

Six native targets are supported for Node.js x64 and ARM64:

- macOS 11+: `x86_64-apple-darwin` produces `native/darwin-x64/libsqlite_queue.dylib`;
  `aarch64-apple-darwin` produces `native/darwin-arm64/libsqlite_queue.dylib`.
- Linux glibc: `x86_64-unknown-linux-gnu` produces `native/linux-x64/libsqlite_queue.so`;
  `aarch64-unknown-linux-gnu` produces `native/linux-arm64/libsqlite_queue.so`.
- Windows MSVC: `x86_64-pc-windows-msvc` produces `native/win32-x64/sqlite_queue.dll`;
  `aarch64-pc-windows-msvc` produces `native/win32-arm64/sqlite_queue.dll`.

Linux CI uses Ubuntu 22.04 as the build baseline for glibc 2.35+.
Alpine/musl and 32-bit prebuilds are not included. Use `extensionPath` or
`QUEUE_EXTENSION` to select a compatible library built separately.

## Local builds

```sh
npm ci
npm run native:list
npm run build:native
npm run native:verify -- --runtime
npm test
```

Without a target option, the script builds for the OS and architecture of the
**Node.js process running the command**. It always supplies Cargo's `--target`,
rather than inferring the output from a directory name or `CARGO_BUILD_TARGET`.
It reads Cargo's JSON artifact paths, so `CARGO_TARGET_DIR` is supported.

Specify a Rust triple, or repeat the flag to build multiple targets:

```sh
# macOS with Xcode/Command Line Tools and both Rust targets installed.
rustup target add aarch64-apple-darwin x86_64-apple-darwin
npm run build:native -- --target aarch64-apple-darwin --target x86_64-apple-darwin

# Windows x64 with Rust, Visual Studio C++ Build Tools, and the Windows SDK.
npm run build:native -- --target x86_64-pc-windows-msvc

# Linux x64 with Rust and a C linker such as GCC.
npm run build:native -- --target x86_64-unknown-linux-gnu
```

Specifying a target does not install its linker or SDK. Install the target's Rust
standard library and native toolchain first, including MSVC ARM64 tools when needed.
Cross-compilation across operating systems requires additional toolchains; the CI
workflow is the recommended approach. `--all` attempts all six targets and requires
all corresponding toolchains on the build machine.

macOS defaults to `MACOSX_DEPLOYMENT_TARGET=11.0`. Override it through the environment
when necessary, and update the published compatibility requirements accordingly.

## Binary and manifest verification

The build script inspects Mach-O, ELF, or PE headers before copying a library to
its platform directory. Each library has a `manifest.json` recording its Rust triple,
version, size, binary SHA-256, and source identity from `src/`, `Cargo.toml`, and
`Cargo.lock`. Source hashing normalizes CRLF so Windows and Unix artifacts can be combined.

```sh
npm run native:verify          # Verify every target currently present.
npm run native:verify -- --all # Require all six targets.
```

Verification rejects mismatched versions/sources, modified binaries, incorrect
architectures, missing manifests, and missing required targets. No placeholder
binaries are generated. Runtime resolution selects the directory using
`process.platform`/`process.arch` and validates its binary header before loading.
GNU Linux prebuilds are not selected automatically on musl.

## GitHub Actions

The workflow is [native.yml](../.github/workflows/native.yml) and runs on pushes,
pull requests, or `workflow_dispatch`:

1. Select native macOS Intel/ARM64, Ubuntu Intel/ARM64, and Windows Intel/ARM64 runners.
2. Install Rust 1.91.1 and Node 22 for the target architecture.
3. Build the real target, verify its manifest/header, and test queues, workers, background maintenance, and types.
4. Test the same binary on Node 24 and upload the target's native artifact.
5. After all six build jobs pass, combine artifacts, verify source/checksums, and create an npm tarball.
6. Install the tarball in a temporary project and verify CommonJS/ESM enqueue, claim, and acknowledgement.

Runner labels follow the [GitHub-hosted runners reference](https://docs.github.com/en/actions/reference/runners/github-hosted-runners).
The workflow does not publish to npm or create a GitHub Release. Download the combined
result from the **sqlite-queue-rs-all-platforms** artifact of a successful run.
Adjust matrix `runs-on` labels if your repository uses different runners.

## Packaging collected artifacts

After placing all real target artifacts in `native/`:

```sh
npm run pack:all
npm run test:package -- --all ./sqlite-queue-rs-0.3.0.tgz
```

`pack:all` requires all six verified targets and packs without rebuilding the host
or running prepack again. The aggregation job does not need Rust/Cargo. Consumers
on supported targets can install that tarball without compiling Rust.

A normal `npm pack` builds the host and includes every target already in `native/`.
It can create a local platform package without claiming to include all platforms.
Local build success does not establish Linux/Windows runtime compatibility until
those systems' CI jobs pass.
