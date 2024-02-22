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
