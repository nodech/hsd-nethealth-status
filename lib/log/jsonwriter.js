/*!
 * jsonwriter.js - Log events into json files.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const Writer = require('./writer');
const {StoreOptions} = require('./common');
const {fileOptions} = require('./jsoncommon');

const NULL_BUFFER = Buffer.from('null\n', 'utf8');

/**
 * @typedef {Object} StoreFile
 * @property {String} name
 * @property {Number} active
 * @property {Number} size
 * @property {Boolean} gzipped
 * @property {Number} time
 */

class JSONWriter extends Writer {
  /**
   * @param {Object} options
   */

  constructor(options) {
    super(new StoreOptions(options), fileOptions);
  }

  /**
   * Write json line.
   * @param {Object} json - JSON object.
   * @param {Number} ts - Log Timestamp.
   * @returns {Promise<Boolean>} - false - if we can't write nor buffer.
   */

  writeJSONLine(json, ts) {
    assert(typeof ts === 'number' && ts > 0, 'Timestamp is required.');
    let data = NULL_BUFFER;

    if (json != null)
      data = Buffer.from(JSON.stringify(json) + '\n', 'utf8');

    return this.write(data, ts);
  }
}

/*
 * Expose
 */

module.exports = JSONWriter;
