/*!
 * common.js - Common utilities for JSONStoreWriter and JSONStoreReader
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const path = require('node:path');
const util = require('node:util');
const assert = require('bsert');
const bfs = require('bfile');
const os = require('node:os');
const {getFileSize} = require('./utils');

const common = exports;

common.FILE_FORMAT = 'event-%d.json%s';
common.FILE_REGEX = /^event-(?<ts>\d+)\.json(?<gz>\.gz)?$/;
common.STORE_NAME = 'events';

class StoreFile {
  constructor(options = {}) {
    this.prefix = options.prefix || '';
    this.filename = options.name || '';
    this.size = options.size || 0;
    this.gzipped = options.gzipped || false;
    this.time = options.time || 0;
  }

  get file() {
    return path.join(this.prefix, this.filename);
  }

  static async fromFileName(prefix, filename) {
    assert(typeof prefix === 'string');
    assert(typeof filename === 'string');

    const match = filename.match(common.FILE_REGEX);

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

class StoreOptions {
  constructor(options) {
    this.name = common.STORE_NAME;
    this.prefix = path.join(os.tmpdir(), 'hsd-nethealth', this.name);
    this.maxFileSize = 50 << 20; // 50MB
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

/**
 * Format file name.
 * @param {Number} time
 * @param {Boolean} gzipped
 * @returns {String}
 */

common.getFileName = (time, gzipped) => {
  return util.format(common.FILE_FORMAT, time, gzipped ? '.gz' : '');
};

/**
 * Get store files.
 * @param {String} prefix
 */

common.getStoreFiles = async function getStoreFiles(prefix) {
  if (!await bfs.exists(prefix))
    return [];

  const files = await bfs.readdir(prefix);
  const filesByTime = new Map();

  for (const file of files) {
    if (!common.FILE_REGEX.test(file))
      continue;

    const storeFile = await StoreFile.fromFileName(prefix, file);

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

  return matchedFiles;
};

common.StoreFile = StoreFile;
common.StoreOptions = StoreOptions;
