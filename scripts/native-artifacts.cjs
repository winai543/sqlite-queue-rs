'use strict';
const { existsSync, readFileSync, writeFileSync, readdirSync } = require('node:fs');
const { createHash } = require('node:crypto');
const path = require('node:path');
const { TARGETS, validateBinary } = require('../lib/native.cjs');

function packageVersion(root) {
  const version = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8')).version;
  const rust = readFileSync(path.join(root, 'Cargo.toml'), 'utf8').match(/^version\s*=\s*"([^"]+)"/m)?.[1];
  if (version !== rust) throw new Error(`Node (${version}) and Rust (${rust}) versions must match before packaging.`);
  return version;
}

function metadata(root, target, file) {
  validateBinary(file, target);
  const buffer = readFileSync(file);
  return { version: packageVersion(root), target: target.triple, platform: target.platform,
    arch: target.arch, libc: target.libc, filename: target.filename,
    sourceSha256: sourceHash(root),
    size: buffer.length, sha256: createHash('sha256').update(buffer).digest('hex') };
}

function sourceHash(root) {
  const files = ['Cargo.toml', 'Cargo.lock'];
  if (existsSync(path.join(root, 'build.rs'))) files.push('build.rs');
  const walk = folder => {
    for (const entry of readdirSync(path.join(root, folder), { withFileTypes: true })) {
      const name = `${folder}/${entry.name}`;
      if (entry.isDirectory()) walk(name);
      else files.push(name);
    }
  };
  walk('src');
  const hash = createHash('sha256');
  for (const file of files.sort()) {
    // Git may check out CRLF on Windows; the source identity must match Linux/macOS.
    hash.update(file).update('\0').update(readFileSync(path.join(root, file), 'utf8').replace(/\r\n/g, '\n')).update('\0');
  }
  return hash.digest('hex');
}

function writeManifest(root, target, file) {
  const info = metadata(root, target, file);
  writeFileSync(path.join(path.dirname(file), 'manifest.json'), `${JSON.stringify(info, null, 2)}\n`);
  return info;
}

function verifyNative(root, { all = false } = {}) {
  const verified = [];
  for (const target of TARGETS) {
    const file = path.join(root, 'native', target.key, target.filename);
    if (!existsSync(file)) {
      if (all) throw new Error(`Missing ${target.key}: build ${target.triple} before creating the all-platform package.`);
      continue;
    }
    const manifestPath = path.join(path.dirname(file), 'manifest.json');
    if (!existsSync(manifestPath)) throw new Error(`Missing ${manifestPath}; rebuild this target.`);
    const manifest = JSON.parse(readFileSync(manifestPath, 'utf8'));
    const actual = metadata(root, target, file);
    for (const key of Object.keys(actual)) {
      if (manifest[key] !== actual[key]) throw new Error(`Native manifest mismatch (${key}) for ${target.key}; rebuild this target.`);
    }
    verified.push({ ...actual, key: target.key });
  }
  if (!verified.length) throw new Error('No native libraries found. Run npm run build:native first.');
  return verified;
}

module.exports = { packageVersion, sourceHash, writeManifest, verifyNative };
