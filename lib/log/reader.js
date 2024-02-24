/*!
 * reader.js - Generic reader interface.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const zlib = require('node:zlib');

const {
  openReadStream,
  closeStream,
  binarySearchFiles
} = require('./utils');

const {getStoreFiles} = require('./common');

class Reader {
  constructor(options = {}, fileOptions = {}) {
    assert(options.prefix, 'Prefix is required.');
    this.options = options;
    this.fileOptions = fileOptions;

    this.lastReadTimestamp = 0;
    this.file = null;
    this.stream = null;
  }

  /**
   * @param {Number} startTS - Start timestamp.
   * @returns {Promise<Reader>}
   */

  async open(startTS = 0) {
    this.lastReadTimestamp = startTS;
    this.startFile = await this.getStartFile(startTS);
    this.file = this.startFile;

    await this.openFile(this.startFile);

    return this;
  }

  /**
   * @param {StoreFile} file - File to open.
   * @returns {Promise<void>}
   */

  async openFile(file) {
    assert(file);

    let stream = await openReadStream(file.path);

    if (file.gzipped)
      stream = stream.pipe(zlib.createGunzip());

    this.stream = stream;
  }

  /**
   * Close reader.
   * @returns {Promise<void>}
   */

  async close() {
    if (this.stream)
      await closeStream(this.stream);

    this.lastReadTimestamp = 0;
  }

  /**
   * Get starting file.
   * @param {Number} startTS - Start timestamp.
   * @returns {Promise<StoreFile>}
   */

  async getStartFile(startTS = 0) {
    // Refetch all the time, in case new files were added.
    const files = await getStoreFiles(this.options.prefix, this.fileOptions);

    if (files.length === 0)
      return null;

    const fileLike = { time: startTS };
    let index = binarySearchFiles(files, fileLike, compare, false);

    if (index < 0)
      index = 0;

    return files[index];
  }

  /**
   * Get next event file.
   * @param {Number} startTS - Start timestamp.
   * @returns {Promise<StoreFile>}
   */

  async getNextFile(startTS = 0) {
    // Refetch all the time, in case new files were added.
    const files = await getStoreFiles(this.options.prefix, this.fileOptions);

    if (files.length === 0)
      return null;

    const fileLike = { time: startTS };
    const index = binarySearchFiles(files, fileLike, compare, true);

    if (index > files.length - 1)
      return null;

    return files[index];
  }

  /**
   * Open next file.
   * @returns {Promise<Boolean>}
   */

  async openNextFile() {
    const nextFile = await this.getNextFile(this.lastReadTimestamp);

    if (nextFile && nextFile.path !== this.file.path) {
      this.file = nextFile;
      await this.openFile(nextFile);
      return true;
    }

    return false;
  }
}

function compare(a, b) {
  return a.time - b.time;
}

module.exports = Reader;
