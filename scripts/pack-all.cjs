'use strict';
const { spawnSync } = require('node:child_process');
const path = require('node:path');
const { verifyNative } = require('./native-artifacts.cjs');
const { verifyInstallPolicy } = require('./package-policy.cjs');

try {
  const root = path.resolve(__dirname, '..');
  verifyInstallPolicy(root);
  const entries = verifyNative(root, { all: true });
  console.log(`Packing all ${entries.length} verified native targets.`);
  if (!process.env.npm_execpath) throw new Error('Run this command through npm run pack:all.');
  // Invoke npm through Node to work on Windows without invoking a .cmd shell.
  const result = spawnSync(process.execPath, [process.env.npm_execpath, 'pack', '--ignore-scripts', ...process.argv.slice(2)],
    { cwd: root, stdio: 'inherit' });
  if (result.error) throw result.error;
  process.exitCode = result.status ?? 1;
} catch (error) { console.error(error.message); process.exitCode = 1; }
