/*!
 * jsoncommon.js - Common utilities for json writer and json reader.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const common = exports;

common.FILE_FORMAT = 'event-%d.json%s';
common.FILE_REGEX = /^event-(?<ts>\d+)\.json(?<gz>\.gz)?$/;
common.STORE_NAME = 'events';
common.EXT = 'json';

common.fileOptions = {
  regex: common.FILE_REGEX,
  format: common.FILE_FORMAT,
  name: common.STORE_NAME,
  ext: common.EXT
};
