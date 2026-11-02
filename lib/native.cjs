'use strict';
const { readFileSync } = require('node:fs');

const TARGETS = Object.freeze([
  ['aarch64-apple-darwin', 'darwin', 'arm64', 'libsqlite_queue.dylib'],
  ['x86_64-apple-darwin', 'darwin', 'x64', 'libsqlite_queue.dylib'],
  ['aarch64-unknown-linux-gnu', 'linux', 'arm64', 'libsqlite_queue.so'],
  ['x86_64-unknown-linux-gnu', 'linux', 'x64', 'libsqlite_queue.so'],
  ['aarch64-pc-windows-msvc', 'win32', 'arm64', 'sqlite_queue.dll'],
  ['x86_64-pc-windows-msvc', 'win32', 'x64', 'sqlite_queue.dll'],
].map(([triple, platform, arch, filename]) => Object.freeze({ triple, platform, arch, filename,
  key: `${platform}-${arch}`, libc: platform === 'linux' ? 'glibc' : null })));

function targetForTriple(triple) {
  const target = TARGETS.find(target => target.triple === triple);
  if (!target) throw new Error(`Unsupported Rust target: ${triple}. Use --list to see supported targets.`);
  return target;
}

function runtimeTarget(platform = process.platform, arch = process.arch, glibc) {
  const target = TARGETS.find(target => target.platform === platform && target.arch === arch);
  if (!target) throw new Error(`Unsupported SQLite queue platform: ${platform}-${arch}.`);
  if (platform === 'linux') {
    const version = glibc ?? (process.platform === 'linux' ? process.report.getReport().header.glibcVersionRuntime : undefined);
    if (!version) throw new Error('Linux prebuilds require glibc. Alpine/musl needs an explicitly supplied extensionPath.');
  }
  return target;
}

function binaryInfo(buffer) {
  const fail = () => { throw new Error('Invalid or unsupported 64-bit native library header.'); };
  if (buffer.length < 64) return fail();
  const arch = (machine, x64, arm64) => machine === x64 ? 'x64' : machine === arm64 ? 'arm64' : 'unknown';
  if (buffer.subarray(0, 4).equals(Buffer.from([0x7f, 0x45, 0x4c, 0x46]))) {
    if (buffer[4] !== 2 || ![1, 2].includes(buffer[5])) return fail();
    const machine = buffer[5] === 1 ? buffer.readUInt16LE(18) : buffer.readUInt16BE(18);
    return { platform: 'linux', architectures: [arch(machine, 62, 183)] };
  }
  if (buffer.readUInt16LE(0) === 0x5a4d) {
    const offset = buffer.readUInt32LE(0x3c);
    if (offset > buffer.length - 6 || buffer.toString('ascii', offset, offset + 4) !== 'PE\0\0') return fail();
    return { platform: 'win32', architectures: [arch(buffer.readUInt16LE(offset + 4), 0x8664, 0xaa64)] };
  }
  const little = buffer.readUInt32LE(0) === 0xfeedfacf;
  if (little || buffer.readUInt32BE(0) === 0xfeedfacf) {
    const machine = little ? buffer.readUInt32LE(4) : buffer.readUInt32BE(4);
    return { platform: 'darwin', architectures: [arch(machine, 0x01000007, 0x0100000c)] };
  }
  // Accept universal macOS libraries when extensionPath points at one.
  const magic = buffer.readUInt32BE(0);
  if (magic === 0xcafebabe || magic === 0xcafebabf) {
    const count = buffer.readUInt32BE(4);
    const stride = magic === 0xcafebabe ? 20 : 32;
    if (!count || count > Math.floor((buffer.length - 8) / stride)) return fail();
    return { platform: 'darwin', architectures: Array.from({ length: count }, (_, index) =>
      arch(buffer.readUInt32BE(8 + index * stride), 0x01000007, 0x0100000c)) };
  }
  return fail();
}

function validateBinary(file, target) {
  const info = binaryInfo(readFileSync(file));
  if (info.platform !== target.platform || !info.architectures.includes(target.arch)) {
    throw new Error(`Native binary mismatch for ${target.key}: ${file} contains ${info.platform}-${info.architectures.join('/')}.`);
  }
  return info;
}

module.exports = { TARGETS, targetForTriple, runtimeTarget, binaryInfo, validateBinary };
