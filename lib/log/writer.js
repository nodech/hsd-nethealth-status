/*!
 * writer.js - Generic writer interface.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 *
 * Parts of this are based on
 * https://github.com/nodech/hsd-bweb-log/blob/f4668a2c1c1e37fa337f8157d3c3aa0a5270f8b6/lib/store/rotating-file.js
 * https://github.com/bcoin-org/blgr/blob/050cbb587a1654a078468dbb92606330fdc4d120/lib/logger.js
 */

'use strict';

const path = require('node:path');
const assert = require('bsert');
const bfs = require('bfile');
const LockFile = require('./lockfile');

const {
  openWriteStream,
  closeStream,
  getFileSize,
  gzipFile
} = require('./utils');

const {
  getFileName,
  getStoreFiles
} = require('./common');

class Writer {
  constructor(options = {}, fileOptions = {}) {
    assert(options.prefix, 'Prefix is required.');
    this.options = options;
    this.fileOptions = fileOptions;

    this._buffer = [];
    this.stream = null;
    this.closed = false;
    this.closing = false;
    this.rotating = false;
    this.opening = false;

    this.ready = false;

    this.filePath = '';
    this.fileSize = 0;
    this.lockFile = new LockFile(bfs, this.options.prefix);
    this.gzipEnabled = this.options.gzip;
  }

  /**
   * Get directory prefix.
   * @returns {String}
   */

  get prefix() {
    return this.options.prefix;
  }

  handleError(e) {
    assert(this.stream);

    try {
      this.stream.close();
    } catch (e) {
      ;
    }

    this.closed = true;
    this.stream = null;
    this.retry();
  }

  /**
   * Open writer. Does not create files.
   * @returns {Promise}
   */

  async open() {
    await this.ensure();
    await this.lockFile.open();
    this.ready = true;
  }

  /**
   * Try closing stream.
   * May not write some data if the file was rotationg.
   * @returns {Promise}
   */

  async close(closeLock = true) {
    assert(!this.closed);
    assert(this.stream);

    this.closing = true;
    try {
      await closeStream(this.stream);
      this._onClose();
    } finally {
      this.closing = false;
    }

    if (closeLock) {
      await this.lockFile.close();
      this.ready = false;
    }

    this.stream = null;
    this.closed = true;
  }

  /**
   * Open file.
   * @param {Number} ts
   * @returns {Promise}
   */

  async openFile(ts) {
    this.opening = true;
    const fileName = getFileName(ts, false, this.fileOptions);
    const filePath = path.join(this.prefix, fileName);
    const fileSize = await getFileSize(filePath);

    this.fileSize = fileSize;
    this.filePath = filePath;

    try {
      this.stream = await openWriteStream(filePath, {
        flags: 'a',
        autoClose: true
      });
    } catch (e) {
      this.retry(ts);
      return;
    }

    this.opening = false;
    this.closed = false;

    this.stream.once('error', e => this.handleError(e));

    while (this._buffer.length > 0 && !this.rotating) {
      const [msg, ts] = this._buffer.shift();

      if (!this.write(msg, ts)) {
        this._buffer.unshift(msg);
        break;
      }
    }
  }

  /**
   * Ensure store has directory.
   * @returns {Promise}
   */

  ensure() {
    return bfs.mkdirp(this.prefix);
  }

  /**
   * Retry opening store.
   * @param {Number} ts
   */

  retry(ts) {
    if (this.timer != null)
      return;

    this.timer = setTimeout(() => {
      this.timer = null;
      this.openFile(ts);
    }, 1000);
  }

  /**
   * Write data to the file. (may rotate)
   * @param {Buffer} data
   * @param {Number} ts
   * @returns {Boolean} - false - if we can't write nor buffer.
   */

  write(data, ts) {
    assert(typeof ts === 'number' && ts > 0, 'Timestamp is required.');
    assert(Buffer.isBuffer(data));

    if (!this.ready)
      return false;

    if (this.rotating || this.opening) {
      this._buffer.push([data, ts]);
      return true;
    }

    if (this.closing)
      return false;

    if (!this.stream) {
      this._buffer.push([data, ts]);
      this.openFile(ts);
      return true;
    }

    this.stream.write(data);
    this.fileSize += data.length;

    if (this.fileSize >= this.options.maxFileSize)
      this.rotate();

    return true;
  }

  /**
   * @private
   * @returns {Promise}
   */

  async rotate() {
    if (this.rotating)
      return;

    if (!this.stream || this.closed)
      return;

    this.rotating = true;
    const last = this.filePath;
    await this.close(false);
    this.rotating = false;

    await this.gzip(last);
  }

  /**
   * @private
   * Gzip file.
   * @param {String} file
   * @returns {Promise}
   */

  async gzip(file) {
    if (!this.options.autoGzip)
      return false;

    try {
      await gzipFile(file);
      await bfs.remove(file);
      return true;
    } catch (e) {
      return false;
    }
  }

  _onClose() {
    ;
  }

  /**
   * Get active file.
   * @returns {Promise<String>}
   */

  async getActiveFile() {
    const files = await this.getFiles();

    if (files.length === 0)
      return this.getNextFile();

    const last = files[files.length - 1];

    if (last.gzipped || last.size >= this.options.maxFileSize)
      return null;

    return last.path;
  }

  /**
   * Get files in prefix.
   * @returns {Promise<StoreFile[]>}
   */

  async getFiles() {
    return getStoreFiles(this.prefix, this.fileOptions);
  }
}

module.exports = Writer;
