/*!
 * common.js - Common utilities for JSONStoreWriter and JSONStoreReader
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const path = require('node:path');
const util = require('node:util');
const assert = require('bsert');
const bfs = require('bfile');
const os = require('node:os');
const {getFileSize} = require('./utils');

const common = exports;

common.STORE_NAME = 'events';

class StoreOptions {
  constructor(options) {
    this.name = common.STORE_NAME;
    this.prefix = path.join(os.tmpdir(), 'hsd-nethealth', this.name);
    this.maxFileSize = 500 << 20; // 500MiB
    this.autoGzip = true;

    if (options)
      this.fromOptions(options);
  }

  fromOptions(options) {
    assert(options);

    if (options.name != null) {
      assert(typeof options.name === 'string');
      this.name = options.name;
    }

    if (options.prefix != null) {
      assert(typeof options.prefix === 'string');
      this.prefix = path.join(options.prefix, this.name);
    }

    if (options.maxFileSize != null) {
      assert(typeof options.maxFileSize === 'number');
      this.maxFileSize = options.maxFileSize;
    }

    if (options.autoGzip != null) {
      assert(typeof options.autoGzip === 'boolean');
      this.autoGzip = options.autoGzip;
    }

    return this;
  }
}

class StoreFile {
  constructor(options = {}) {
    this.prefix = options.prefix || '';
    this.filename = options.name || '';
    this.size = options.size || 0;
    this.gzipped = options.gzipped || false;
    this.time = options.time || 0;
  }

  get path() {
    return path.join(this.prefix, this.filename);
  }

  static async fromFileName(prefix, filename, options = {}) {
    assert(typeof prefix === 'string');
    assert(typeof filename === 'string');

    const match = filename.match(options.regex);

    if (!match)
      throw new Error(`Invalid file name: ${filename}`);

    const full = path.join(prefix, filename);
    const file = new StoreFile({
      prefix: prefix,
      name: filename,
      size: await getFileSize(full),
      gzipped: match.groups.gz != null,
      time: Number(match.groups.ts)
    });

    return file;
  }
}

/**
 * Format file name.
 * @param {Number} time
 * @param {Boolean} gzipped
 * @param {Object} options
 * @returns {String}
 */

common.getFileName = (time, gzipped, options) => {
  return util.format(options.format, time, gzipped ? '.gz' : '');
};

/**
 * Get store files.
 * @param {String} prefix
 * @param {Object} options
 * @returns {Promise<StoreFile[]>}
 */

common.getStoreFiles = async function getStoreFiles(prefix, options = {}) {
  if (!await bfs.exists(prefix))
    return [];

  const files = await bfs.readdir(prefix);
  const filesByTime = new Map();

  for (const file of files) {
    if (!options.regex.test(file))
      continue;

    const storeFile = await StoreFile.fromFileName(prefix, file, options);

    if (filesByTime.has(storeFile.time)) {
      const prev = filesByTime.get(storeFile.time);

      if (!prev.gzipped && storeFile.gzipped)
        filesByTime.set(storeFile.time, storeFile);

      continue;
    } else {
      filesByTime.set(storeFile.time, storeFile);
    }
  }

  const matchedFiles = [];

  for (const file of filesByTime.values())
    matchedFiles.push(file);

  files.sort((a, b) => a.time - b.time);

  return matchedFiles;
};

/**
 * Get start timestamp.
 * @param {String} prefix
 * @param {Object} options
 * @returns {Promise<Number>}
 */

common.getStartTS = async function getStartTS(prefix, options = {}) {
  const files = await common.getStoreFiles(prefix, options);

  if (files.length === 0)
    return -1;

  return files[0].time;
};

common.StoreFile = StoreFile;
common.StoreOptions = StoreOptions;

common.ERRORS = {
  ECONNREFUSED: 3,
  EHOSTUNREACH: 4,
  ENETUNREACH: 5,
  ECONNRESET: 6,

  CONN_TIMEOUT: 101,
  HANGUP: 102,
  STALLING: 103,
  TOTAL_TIMEOUT: 104,

  PROTOCOL_INVALID_MAGIC: 200
};

common.error2code = function error2code(message) {
  if (message.includes('ECONNREFUSED'))
    return common.ERRORS.ECONNREFUSED;

  if (message.includes('EHOSTUNREACH'))
    return common.ERRORS.EHOSTUNREACH;

  if (message.includes('ENETUNREACH'))
    return common.ERRORS.ENETUNREACH;

  if (message.includes('ECONNRESET'))
    return common.ERRORS.ECONNRESET;

  if (message.includes('Connection timed out.'))
    return common.ERRORS.CONN_TIMEOUT;

  if (message.includes('Socket hangup'))
    return common.ERRORS.HANGUP;

  if (message.includes('Peer is stalling'))
    return common.ERRORS.STALLING;

  if (message.includes('Timeout'))
    return common.ERRORS.TOTAL_TIMEOUT;

  if (message.includes('Invalid magic value'))
    return common.ERRORS.PROTOCOL_INVALID_MAGIC;

  throw new Error(`Unknown error: ${message}`);
};

common.code2error = function code2error(code) {
  switch (code) {
    case common.ERRORS.ECONNREFUSED:
      return 'ECONNREFUSED';
    case common.ERRORS.EHOSTUNREACH:
      return 'EHOSTUNREACH';
    case common.ERRORS.ENETUNREACH:
      return 'ENETUNREACH';
    case common.ERRORS.ECONNRESET:
      return 'ECONNRESET';
    case common.ERRORS.CONN_TIMEOUT:
      return 'TIMEOUT';
    case common.ERRORS.HANGUP:
      return 'HANGUP';
    case common.ERRORS.STALLING:
      return 'STALLING';
    case common.ERRORS.TOTAL_TIMEOUT:
      return 'TOTAL_TIMEOUT';
    case common.ERRORS.PROTOCOL_INVALID_MAGIC:
      return 'PROTOCOL_INVALID_MAGIC';
    default:
      throw new Error(`Unknown code: ${code}`);
  }
};
