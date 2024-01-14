/*!
 * log.js - Log events into json files.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 *
 * Parts of this are based on
 * https://github.com/nodech/hsd-bweb-log/blob/f4668a2c1c1e37fa337f8157d3c3aa0a5270f8b6/lib/store/rotating-file.js
 * https://github.com/bcoin-org/blgr/blob/050cbb587a1654a078468dbb92606330fdc4d120/lib/logger.js
 */

'use strict';

const assert = require('bsert');
const os = require('node:os');
const util = require('node:util');
const path = require('node:path');
const zlib = require('node:zlib');
const bfs = require('bfile');
const LockFile = require('./lockfile');

const FILE_FORMAT = 'event-%d.json%s';
const FILE_REGEX = /^event-(?<ts>\d+)\.json(?<gz>\.gz)?$/;
const STORE_NAME = 'events';

/**
 * @typedef {Object} StoreFile
 * @property {String} name
 * @property {Number} active
 * @property {Number} size
 * @property {Boolean} gzipped
 * @property {Number} time
 */

class JSONStore {
  /**
   * @param {Object} options
   */

  constructor(options) {
    this.options = new JSONStoreOptions(options);

    this._buffer = [];
    this.stream = null;
    this.closed = false;
    this.closing = false;
    this.rotating = false;
    this.fileName = '';
    this.fileSize = 0;
    this.lockFile = new LockFile(bfs, this.options.prefix);

    this.timer = null;
  }

  handleError() {
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
   * Open store.
   * @returns {Promise}
   */

  async open() {
    await this.ensure();
    await this.lockFile.open();

    this.fileName = await this.getActiveFile();
    this.fileSize = await getFileSize(this.fileName);

    try {
      this.stream = await openStream(this.fileName, {
        flags: 'a',
        autoClose: true
      });
    } catch (e) {
      this.retry();
      return;
    }

    this.closed = false;
    this.stream.once('error', e => this.handleError(e));

    while (this._buffer.length > 0 && !this.rotating) {
      const msg = this._buffer.shift();

      if (!this.write(msg)) {
        this._buffer.unshift(msg);
        break;
      }
    }
  }

  /**
   * Try closing stream.
   * May not write some data if the file was rotationg.
   * @returns {Promise}
   */

  async close() {
    assert(!this.closed);
    assert(this.stream);

    this.closing = true;
    try {
      await closeStream(this.stream);
    } finally {
      this.closing = false;
    }

    await this.lockFile.close();

    this.stream = null;
    this.closed = true;
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
   */

  retry() {
    if (this.timer != null)
      return;

    this.timer = setTimeout(() => {
      this.timer = null;
      this.open();
    }, 1000);
  }

  /**
   * Get directory prefix.
   * @returns {String}
   */

  get prefix() {
    return this.options.prefix;
  }

  /**
   * Get active file.
   * @returns {Promise<String>}
   */

  async getActiveFile() {
    const files = await this.getFiles();

    if (files.length === 0)
      return this.getNextFile();

    files.sort((a, b) => a.time - b.time);

    const last = files[files.length - 1];

    if (last.gzipped || last.size >= this.options.maxFileSize)
      return this.getNextFile();

    return last.file;
  }

  /**
   * Get next file.
   * @returns {String}
   */

  getNextFile() {
    return path.join(this.prefix, JSONStore.getFileName(Date.now(), false));
  }

  /**
   * Write json line.
   * @returns {Promise<Boolean>} - false - if we can't write nor buffer.
   */

  writeJSONLine(json) {
    if (json == null)
      return this.write('null\n');

    return this.write(JSON.stringify(json) + '\n');
  }

  /**
   * Write data to the file. (may rotate)
   * @param {String} msg
   * @returns {Boolean} - false - if we can't write nor buffer.
   */

  write(data) {
    if (!this.stream && !this.rotating)
      return false;

    if (this.closing && !this.rotating)
      return false;

    if (this.rotating) {
      this._buffer.push(data);
      return true;
    }

    if (!Buffer.isBuffer(data))
      data = Buffer.from(data, 'utf8');

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
    const last = this.fileName;
    await this.close();
    await this.open();
    this.rotating = false;

    const newName = this.fileName;

    if (last === newName)
      return;

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

  /**
   * Get files in prefix.
   * @returns {Promise<StoreFile[]>}
   */

  async getFiles() {
    return JSONStore.getFiles(this.prefix);
  }

  /**
   * Get store files.
   * @param {Object} prefix
   * @returns {Promise<StoreFile[]>}
   */

  static async getFiles(prefix) {
    if (!await bfs.exists(prefix))
      return [];

    const files = await bfs.readdir(prefix);
    const filesByTime = new Map();

    for (const file of files) {
      if (!FILE_REGEX.test(file))
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
  }

  /**
   * Get file name.
   * @param {Number} time
   * @param {Boolean} gzipped
   * @returns {String}
   */

  static getFileName(time, gzipped) {
    return util.format(FILE_FORMAT, time, gzipped ? '.gz' : '');
  }
}

class JSONStoreOptions {
  constructor(options) {
    this.name = STORE_NAME;
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

    const match = filename.match(FILE_REGEX);

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

async function getFileSize(file) {
  try {
    const stat = await bfs.stat(file);

    return stat.size;
  } catch (e) {
    if (e.code === 'ENOENT')
      return 0;

    throw e;
  }
}

function openStream(filename, flags) {
  return new Promise((resolve, reject) => {
    const stream = bfs.createWriteStream(filename, flags);

    const cleanup = () => {
      /* eslint-disable */
      stream.removeListener('error', onError);
      stream.removeListener('open', onOpen);
      /* eslint-enable */
    };

    const onError = (err) => {
      try {
        stream.close();
      } catch (e) {
        ;
      }
      cleanup();
      reject(err);
    };

    const onOpen = () => {
      cleanup();
      resolve(stream);
    };

    stream.once('error', onError);
    stream.once('open', onOpen);
  });
}

function closeStream(stream) {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      /* eslint-disable */
      stream.removeListener('error', onError);
      stream.removeListener('close', onClose);
      /* eslint-enable */
    };

    const onError = (err) => {
      cleanup();
      reject(err);
    };

    const onClose = () => {
      cleanup();
      resolve(stream);
    };

    stream.removeAllListeners('error');
    stream.removeAllListeners('close');
    stream.once('error', onError);
    stream.once('close', onClose);

    stream.close();
  });
}

function gzipFile(file) {
  assert(!file.endsWith('.gz'));

  return new Promise((resolve, reject) => {
    const out = file + '.gz';
    const readStream = bfs.createReadStream(file);
    const writeStream = bfs.createWriteStream(out);
    const gzip = zlib.createGzip();

    readStream
      .pipe(gzip)
      .pipe(writeStream)
      .once('finish', () => resolve(out))
      .once('error', reject);
  });
}

/*
 * Expose
 */

JSONStore.StoreFile = StoreFile;
JSONStore.StoreOptions = JSONStoreOptions;

module.exports = JSONStore;
