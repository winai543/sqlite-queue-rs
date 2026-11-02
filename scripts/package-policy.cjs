'use strict';
const { existsSync, readFileSync } = require('node:fs');
const path = require('node:path');

function verifyInstallPolicy(root) {
  const pkg = JSON.parse(readFileSync(path.join(root, 'package.json'), 'utf8'));
  for (const hook of ['preinstall', 'install', 'postinstall', 'prepare']) {
    if (pkg.scripts?.[hook]) throw new Error(`Remove ${hook}: consumers must install this package without a build step.`);
  }
  if (existsSync(path.join(root, 'binding.gyp'))) {
    throw new Error('binding.gyp can trigger an implicit node-gyp build during installation.');
  }
  return pkg;
}

module.exports = { verifyInstallPolicy };
