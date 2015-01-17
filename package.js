Package.describe({
  git: 'https://github.com/redpost/Meteor-cfs-worker.git',
  name: 'cfs:worker',
  version: '0.1.3',
  summary: 'CollectionFS, file worker - handles file copies/versions'
});

Package.onUse(function(api) {
  api.versionsFrom('1.0');

  api.use([
    'cfs:base-package@0.0.27',
    'cfs:tempstore@0.1.3',
    'cfs:storage-adapter@0.1.1'
  ]);

  api.use([
    'livedata',
    'mongo-livedata',
    'cfs:power-queue@0.9.11'
  ]);

  api.addFiles([
    'fileWorker.js'
  ], 'server');
});

// Package.on_test(function (api) {
//   api.use('cfs:standard-packages@0.0.0');

//   api.use('test-helpers', 'server');
//   api.use(['tinytest', 'underscore', 'ejson', 'ordered-dict', 'random']);

//   api.addFiles('tests/client-tests.js', 'client');
//   api.addFiles('tests/server-tests.js', 'server');
// });
