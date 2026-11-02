'use strict';
const path = require('node:path');
const { buildNative } = require('./build-native.cjs');
const { verifyNative } = require('./native-artifacts.cjs');
const { verifyInstallPolicy } = require('./package-policy.cjs');

try {
  verifyInstallPolicy(path.resolve(__dirname, '..'));
  buildNative([]);
  verifyNative(path.resolve(__dirname, '..'));
} catch (error) { console.error(error.message); process.exitCode = 1; }
