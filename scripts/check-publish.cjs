'use strict';
const path = require('node:path');
const { verifyNative } = require('./native-artifacts.cjs');
const { verifyInstallPolicy } = require('./package-policy.cjs');

try {
  const root = path.resolve(__dirname, '..');
  verifyInstallPolicy(root);
  const targets = verifyNative(root, { all: true });
  console.log(`Publish check passed: ${targets.length} native targets, no consumer install/build hooks.`);
} catch (error) { console.error(error.message); process.exitCode = 1; }
