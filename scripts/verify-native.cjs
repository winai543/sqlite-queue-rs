'use strict';
const path = require('node:path');
const { runtimeTarget } = require('../lib/native.cjs');
const { verifyNative } = require('./native-artifacts.cjs');

try {
  const args = process.argv.slice(2);
  if (args.some(arg => !['--all', '--runtime'].includes(arg))) throw new Error('Usage: verify-native.cjs [--all] [--runtime]');
  const entries = verifyNative(path.resolve(__dirname, '..'), { all: args.includes('--all') });
  if (args.includes('--runtime') && !entries.some(entry => entry.key === runtimeTarget().key)) {
    throw new Error('The native build does not match this Node.js runtime architecture.');
  }
  console.log(`Verified native libraries: ${entries.map(entry => entry.key).join(', ')}`);
} catch (error) { console.error(error.message); process.exitCode = 1; }
