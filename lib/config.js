/*!
 * config.js - Global configs for scripts.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const Config = require('bcfg');

exports.getConfigs = (options) => {
  const config = new Config('nethealth-status', {
    suffix: 'network',
    fallback: 'main',
    alias: {
      'n': 'network'
    }
  });

  config.inject(options);
  config.load(options);

  config.open('nethealth-status.conf');

  if (config.has('config'))
    config.open(config.path('config'));

  return config;
};
