/*!
 * reader.js - Read stored files event by event.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const zlib = require('node:zlib');
const EventEmitter = require('node:events');
const readline = require('node:readline');
const assert = require('bsert');

const {
  openReadStream,
  closeStream,
  binarySearchFiles
} = require('./utils');

const {
  StoreOptions,
  getStoreFiles
} = require('./common');

class Reader extends EventEmitter {
  constructor(options) {
    super();
    this.options = new StoreOptions(options);
    this.lastTimestamp = 0;
    this.stream = null;
  }

  /**
   * @param {Number} startTS - Start timestamp.
   * @returns {Promise<Reader>}
   */

  async open(startTS = 0) {
    this.lastTimestamp = startTS;
    this.startFile = await this.getStartFile(startTS);

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

    this.lastTimestamp = 0;
  }

  /**
   * Get starting file.
   * @param {Number} startTS - Start timestamp.
   * @returns {Promise<StoreFile>}
   */

  async getStartFile(startTS = 0) {
    // Refetch all the time, in case new files were added.
    const files = await getStoreFiles(this.options.prefix);

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
    const files = await getStoreFiles(this.options.prefix);

    if (files.length === 0)
      return null;

    const fileLike = { time: startTS };
    const index = binarySearchFiles(files, fileLike, compare, true);

    if (index > files.length - 1)
      return null;

    return files[index];
  }

  async *[Symbol.asyncIterator]() {
    assert(this.stream);

    let hasNext;

    do {
      hasNext = false;

      const rl = readline.createInterface({
        input: this.stream,
        crlfDelay: Infinity
      });

      for await (const data of rl) {
        const json = JSON.parse(data);

        if (json.logTimestamp <= this.lastTimestamp)
          continue;

        this.lastTimestamp = json.logTimestamp;
        yield json;
      }

      const nextFile = await this.getNextFile(this.lastTimestamp);

      if (nextFile) {
        await this.openFile(nextFile);
        hasNext = true;
      }
    } while (hasNext);
  }
}

function compare(a, b) {
  return a.time - b.time;
}

/*
 * Expose
 */

module.exports = Reader;
