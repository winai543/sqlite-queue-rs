'use strict';
const { spawnSync } = require('node:child_process');
const { mkdtempSync, writeFileSync, rmSync } = require('node:fs');
const { tmpdir } = require('node:os');
const path = require('node:path');
const { verifyNative } = require('./native-artifacts.cjs');
const { verifyInstallPolicy } = require('./package-policy.cjs');

function run(command, args, cwd, env = process.env) {
  const result = spawnSync(command, args, { cwd, env, stdio: 'inherit' });
  if (result.error) throw result.error;
  if (result.status !== 0) throw new Error(`Package smoke test exited with code ${result.status}.`);
}

let folder;
try {
  const args = process.argv.slice(2);
  const all = args.includes('--all');
  const filenames = args.filter(arg => arg !== '--all');
  if (filenames.length !== 1) throw new Error('Usage: npm run test:package -- [--all] <archive.tgz>');
  if (!process.env.npm_execpath) throw new Error('Run this command through npm run test:package.');
  const archive = path.resolve(filenames[0]);
  folder = mkdtempSync(path.join(tmpdir(), 'sqlite-queue-package-'));
  writeFileSync(path.join(folder, 'package.json'), JSON.stringify({ name: 'sqlite-queue-consumer', private: true }));
  // Test a normal npm install, with Rust/Cargo absent from PATH. Node and npm
  // remain available; do not use --ignore-scripts to hide a consumer build hook.
  const consumerEnv = { ...process.env };
  for (const key of Object.keys(consumerEnv)) {
    if (key.toLowerCase() === 'path' || key === 'QUEUE_EXTENSION') delete consumerEnv[key];
  }
  consumerEnv.PATH = path.dirname(process.execPath);
  for (const tool of ['cargo', 'rustc']) {
    const probe = spawnSync(tool, ['--version'], { env: consumerEnv });
    if (probe.error?.code !== 'ENOENT') throw new Error(`Cannot isolate ${tool} from the consumer smoke test PATH.`);
  }
  run(process.execPath, [process.env.npm_execpath, 'install', archive, '--offline', '--no-audit', '--no-fund'], folder, consumerEnv);
  const installed = path.join(folder, 'node_modules', 'sqlite-queue-rs');
  verifyInstallPolicy(installed);
  verifyNative(installed, { all });
  const common = `
    const store = new SQLiteQueue(':memory:');
    try {
      assert.match(resolveExtension(), /node_modules[\\\\/]sqlite-queue-rs[\\\\/]native/);
      const queue=store.queue('smoke');
      const id=queue.push({installed:true});
      const job=queue.claim();
      assert.equal(job.id,id);
      assert.equal(job.data.installed,true);
      assert.equal(queue.ack(id,job.receipt),true);
    } finally { store.close(); }
  `;
  run(process.execPath, ['-e', `const assert=require('node:assert/strict'); const {SQLiteQueue,resolveExtension}=require('sqlite-queue-rs'); ${common}`], folder, consumerEnv);
  run(process.execPath, ['--input-type=module', '-e', `import assert from 'node:assert/strict'; import {SQLiteQueue,resolveExtension} from 'sqlite-queue-rs'; ${common}`], folder, consumerEnv);
  console.log('Installed tarball without Rust/Cargo: normal npm install, native manifests, CommonJS and ESM enqueue/claim/ack verified.');
} catch (error) { console.error(error.message); process.exitCode = 1; }
finally { if (folder) rmSync(folder, { recursive: true, force: true }); }
