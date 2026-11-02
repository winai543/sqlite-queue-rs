# Publishing without consumer Rust builds

GitHub Actions builds Rust and includes the real `.dylib`, `.so`, and `.dll` files
in the npm tarball. Consumers install from the npm registry, and Node.js selects
the binary for their OS and architecture. They do not need Rust, Cargo, a C compiler,
or native build tools. Supported targets are macOS, Linux glibc, and Windows on
x64/ARM64; see the [build guide](BUILD.md).

## Before pushing

- Confirm that your npm account can publish the name `sqlite-queue-rs`.
- Keep the real GitHub repository URL in `package.json`.
- Choose the license you want consumers to receive. The current license is `UNLICENSED`.
- Keep versions aligned in `package.json`, `package-lock.json`, `Cargo.toml`, and `Cargo.lock`.

Commit source, lockfiles, and `.github/workflows/native.yml`. Generated `node_modules/`,
`target/`, and `native/` are ignored because CI produces the binaries.
Rebuild every native target when changing the release version.

## First release

1. Push the project to GitHub.
2. Open **Actions → Native builds** and wait for all six build jobs and **Package all platforms** to pass.
3. Download and unzip **sqlite-queue-rs-all-platforms**. Its `.tgz` contains all targets and has passed the package smoke test.
4. On a machine with Node.js/npm, log in and publish that exact tarball:

```sh
npm login
npm publish ./sqlite-queue-rs-0.3.0.tgz --access public
```

The publishing machine does not need to rebuild Rust. npm supports publishing a
gzipped tarball directly; see the [npm publish documentation](https://docs.npmjs.com/cli/v11/commands/npm-publish/).
Use the complete CI artifact for a release that advertises all supported platforms.
The locally built development tarball may contain only the targets built on that machine.
A successfully published name/version cannot be reused; increment the version for a new release.

## Consumer installation

```sh
npm install sqlite-queue-rs
```

```js
import { SQLiteQueue } from 'sqlite-queue-rs';
const store = new SQLiteQueue('jobs.db');
const email = store.queue('email');
email.push({ to: 'alice@example.com' });
store.close();
```

Consumers need Node.js 22.13+ and a supported OS/architecture, without Rust.
GitHub URL dependencies are source dependencies and do not automatically contain the
ignored `native/` artifacts produced by CI. Install published releases from the npm registry.

## Release checks

- `prepublishOnly` requires all six binaries, manifests, matching source/version, and checksums before publishing a source directory.
- `pack:all` applies the same completeness checks before creating a release tarball.
- Consumer `preinstall`, `install`, `postinstall`, and `prepare` hooks are prohibited, as is `binding.gyp` that could cause an implicit build.
- The package smoke test performs a normal `npm install` with Cargo/Rust removed from `PATH`, then checks loading and CommonJS/ESM enqueue, claim, and acknowledgement.

`prepack` is a producer step, not a registry consumer install hook. Publishing a
tarball uses the artifact already verified by CI rather than source-directory hooks.

## Future automated publishing

The current workflow builds, tests, and packs; it does not publish automatically.
To add publication from GitHub, use npm Trusted Publishing (OIDC), configure the
actual owner/repository/workflow identity, and grant the publishing job `id-token: write`.
Use Node 24 with npm 11.5.1+ to publish the tested tarball without rebuilding it.
See [npm Trusted Publishing](https://docs.npmjs.com/trusted-publishers/) for account configuration.
