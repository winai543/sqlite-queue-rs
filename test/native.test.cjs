'use strict';
const { test } = require('node:test');
const assert = require('node:assert/strict');
const { mkdtempSync, mkdirSync, writeFileSync, readFileSync, rmSync } = require('node:fs');
const path = require('node:path');
const { tmpdir } = require('node:os');
const { TARGETS, targetForTriple, runtimeTarget, binaryInfo, validateBinary } = require('../lib/native.cjs');
const { parseTargets } = require('../scripts/build-native.cjs');
const { writeManifest, verifyNative, sourceHash } = require('../scripts/native-artifacts.cjs');
const { verifyInstallPolicy } = require('../scripts/package-policy.cjs');

// Header fixtures test cross-platform detection without requiring foreign binaries
// to execute on this machine. They are never copied into the package's native/.
function header(target) {
  const buffer = Buffer.alloc(256);
  if (target.platform === 'darwin') {
    buffer.writeUInt32LE(0xfeedfacf, 0);
    buffer.writeUInt32LE(target.arch === 'x64' ? 0x01000007 : 0x0100000c, 4);
  } else if (target.platform === 'linux') {
    buffer.set([0x7f, 0x45, 0x4c, 0x46, 2, 1]);
    buffer.writeUInt16LE(target.arch === 'x64' ? 62 : 183, 18);
  } else {
    buffer.writeUInt16LE(0x5a4d, 0);
    buffer.writeUInt32LE(0x80, 0x3c);
    buffer.write('PE\0\0', 0x80, 'ascii');
    buffer.writeUInt16LE(target.arch === 'x64' ? 0x8664 : 0xaa64, 0x84);
  }
  return buffer;
}

function workspace(t) {
  const root = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-build-test-'));
  mkdirSync(path.join(root, 'src'));
  writeFileSync(path.join(root, 'package.json'), JSON.stringify({ version: '1.0.0' }));
  writeFileSync(path.join(root, 'Cargo.toml'), '[package]\nversion = "1.0.0"\n');
  writeFileSync(path.join(root, 'Cargo.lock'), '# fixture\n');
  writeFileSync(path.join(root, 'src', 'lib.rs'), '// source\n');
  t.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

function add(root, target) {
  const folder = path.join(root, 'native', target.key);
  mkdirSync(folder, { recursive: true });
  const file = path.join(folder, target.filename);
  writeFileSync(file, header(target));
  writeManifest(root, target, file);
  return file;
}

test('target CLI maps all six Rust triples, repeated flags and default runtime', () => {
  assert.equal(TARGETS.length, 6);
  for (const target of TARGETS) {
    assert.equal(parseTargets(['--target', target.triple])[0], target);
    assert.equal(runtimeTarget(target.platform, target.arch, '2.35'), target);
  }
  assert.equal(parseTargets([])[0], runtimeTarget());
  assert.deepEqual(parseTargets(['--all']), TARGETS);
  assert.equal(parseTargets(['--list']), null);
  assert.equal(parseTargets(['--target=x86_64-pc-windows-msvc'])[0].key, 'win32-x64');
  assert.equal(parseTargets(['--target', TARGETS[0].triple, '--target', TARGETS[0].triple]).length, 1);
  for (const args of [['--target'], ['--all', '--target', TARGETS[0].triple], ['--target', 'unknown']]) {
    assert.throws(() => parseTargets(args));
  }
  assert.throws(() => runtimeTarget('linux', 'x64', false), /musl/);
  assert.throws(() => runtimeTarget('win32', 'ia32'), /Unsupported/);
});

test('Mach-O, ELF and PE architecture headers are verified before copying', t => {
  const root = workspace(t);
  for (const target of TARGETS) {
    const file = add(root, target);
    assert.deepEqual(binaryInfo(header(target)), { platform: target.platform, architectures: [target.arch] });
    assert.doesNotThrow(() => validateBinary(file, target));
    const different = TARGETS.find(other => other.platform === target.platform && other.arch !== target.arch);
    assert.throws(() => validateBinary(file, different), /mismatch/);
  }
  assert.throws(() => binaryInfo(Buffer.alloc(12)), /header/);
  const malformed = header(targetForTriple('x86_64-pc-windows-msvc'));
  malformed.writeUInt32LE(0xffffffff, 0x3c);
  assert.throws(() => binaryInfo(malformed), /header/);
  const universal = Buffer.alloc(64);
  universal.writeUInt32BE(0xcafebabe, 0);
  universal.writeUInt32BE(2, 4);
  universal.writeUInt32BE(0x01000007, 8);
  universal.writeUInt32BE(0x0100000c, 28);
  assert.deepEqual(binaryInfo(universal).architectures, ['x64', 'arm64']);
});

test('all-platform packaging refuses missing, mislabeled or modified artifacts', t => {
  const root = workspace(t);
  const first = add(root, TARGETS[0]);
  assert.equal(verifyNative(root).length, 1);
  assert.throws(() => verifyNative(root, { all: true }), /Missing/);
  for (const target of TARGETS.slice(1)) add(root, target);
  assert.equal(verifyNative(root, { all: true }).length, 6);
  const original = readFileSync(first);
  const changed = Buffer.from(original); changed[100] = 1;
  writeFileSync(first, changed);
  assert.throws(() => verifyNative(root, { all: true }), /mismatch/);
  writeFileSync(first, original);
  writeFileSync(path.join(path.dirname(first), 'manifest.json'), '{}');
  assert.throws(() => verifyNative(root), /mismatch/);
});

test('source identity rejects stale builds and normalizes Windows CRLF checkouts', t => {
  const root = workspace(t);
  const hash = sourceHash(root);
  add(root, TARGETS[0]);
  writeFileSync(path.join(root, 'src', 'lib.rs'), '// source\r\n');
  assert.equal(sourceHash(root), hash);
  assert.doesNotThrow(() => verifyNative(root));
  writeFileSync(path.join(root, 'src', 'lib.rs'), '// different source\n');
  assert.throws(() => verifyNative(root), /sourceSha256/);
  writeFileSync(path.join(root, 'package.json'), '{"version":"2.0.0"}');
  assert.throws(() => verifyNative(root), /versions must match/);
});

test('consumer install policy rejects build hooks and implicit node-gyp', t => {
  const root = workspace(t);
  assert.doesNotThrow(() => verifyInstallPolicy(root));
  for (const hook of ['preinstall', 'install', 'postinstall', 'prepare']) {
    writeFileSync(path.join(root, 'package.json'), JSON.stringify({ scripts: { [hook]: 'cargo build' } }));
    assert.throws(() => verifyInstallPolicy(root), /consumers must install/);
  }
  writeFileSync(path.join(root, 'package.json'), JSON.stringify({ scripts: { prepack: 'build publisher artifacts' } }));
  assert.doesNotThrow(() => verifyInstallPolicy(root));
  writeFileSync(path.join(root, 'binding.gyp'), '{}');
  assert.throws(() => verifyInstallPolicy(root), /node-gyp/);
});
