'use strict';
const { spawnSync } = require('node:child_process');
const { mkdirSync, copyFileSync } = require('node:fs');
const path = require('node:path');
const { TARGETS, runtimeTarget, targetForTriple, validateBinary } = require('../lib/native.cjs');
const { packageVersion, writeManifest } = require('./native-artifacts.cjs');

function parseTargets(args) {
  if (args.length === 1 && args[0] === '--list') return null;
  if (args.length === 1 && args[0] === '--all') return TARGETS;
  const triples = [];
  for (let index = 0; index < args.length; index++) {
    if (args[index] === '--target' && args[index + 1]) triples.push(args[++index]);
    else if (args[index].startsWith('--target=')) triples.push(args[index].slice('--target='.length));
    else throw new Error('Usage: build-native.cjs [--target <triple> ... | --list | --all]');
  }
  return triples.length ? [...new Set(triples)].map(targetForTriple) : [runtimeTarget()];
}

function buildNative(args) {
  const targets = parseTargets(args);
  if (!targets) {
    for (const target of TARGETS) console.log(`${target.triple} => native/${target.key}/${target.filename}`);
    return [];
  }
  const root = path.resolve(__dirname, '..');
  packageVersion(root);
  const outputs = [];
  for (const target of targets) {
    const env = { ...process.env };
    if (target.platform === 'darwin') env.MACOSX_DEPLOYMENT_TARGET ??= '11.0';
    console.log(`Building ${target.triple} (${target.key})`);
    const result = spawnSync('cargo', ['build', '--release', '--locked', '--target', target.triple,
      '--message-format=json-render-diagnostics'],
    { cwd: root, env, encoding: 'utf8', stdio: ['ignore', 'pipe', 'inherit'], maxBuffer: 16 * 1024 * 1024 });
    if (result.error) throw new Error('Building the native extension requires Rust/Cargo on PATH.', { cause: result.error });
    if (result.status !== 0) throw new Error(`Build failed for ${target.triple}. Install its Rust target, linker and platform SDK before cross-compiling.`);
    const artifacts = result.stdout.trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
    const artifact = artifacts.find(message => message.reason === 'compiler-artifact'
      && message.target.name === 'sqlite_queue' && message.target.crate_types.includes('cdylib'));
    const source = artifact?.filenames.find(file => path.basename(file) === target.filename);
    if (!source) throw new Error(`Cargo did not produce ${target.filename} for ${target.triple}.`);
    // Inspect the produced bytes before copying; never label a host library as a
    // different OS/architecture merely because a CLI option requested that target.
    validateBinary(source, target);
    const folder = path.join(root, 'native', target.key);
    mkdirSync(folder, { recursive: true });
    const destination = path.join(folder, target.filename);
    copyFileSync(source, destination);
    outputs.push(writeManifest(root, target, destination));
    console.log(`Native extension: ${path.relative(root, destination)}`);
  }
  return outputs;
}

if (require.main === module) {
  try { buildNative(process.argv.slice(2)); }
  catch (error) { console.error(error.message); process.exitCode = 1; }
}

module.exports = { buildNative, parseTargets };
