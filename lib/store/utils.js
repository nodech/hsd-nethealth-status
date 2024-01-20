/*!
 * utils.js - Utilities for JSONStoreWriter and JSONStoreReader
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const zlib = require('node:zlib');
const assert = require('bsert');
const bfs = require('bfile');

const utils = exports;

utils.closeStream = function closeStream(stream) {
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
};

utils.openStream = function openStream(stream) {
  return new Promise((resolve, reject) => {
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
};

utils.openWriteStream = function openWriteStream(filename, flags) {
  const stream = bfs.createWriteStream(filename, flags);
  return utils.openStream(stream);
};

utils.openReadStream = function openReadStream(filename, flags) {
  const stream = bfs.createReadStream(filename, flags);
  return utils.openStream(stream);
};

utils.gzipFile = function gzipFile(file) {
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
};

utils.getFileSize = async function getFileSize(file) {
  try {
    const stat = await bfs.stat(file);

    return stat.size;
  } catch (e) {
    if (e.code === 'ENOENT')
      return 0;

    throw e;
  }
};

/**
 * Perform a binary search on a sorted array.
 * @param {Array} items
 * @param {Object} key
 * @param {Function} compare
 * @param {Boolean?} next
 * @returns {Number} Index.
 */

utils.binarySearchFiles = function binarySearchFiles(items, key, compare, next) {
  let start = 0;
  let end = items.length - 1;

  while (start <= end) {
    const pos = (start + end) >>> 1;
    const cmp = compare(items[pos], key);

    if (cmp === 0)
      return pos;

    if (cmp < 0)
      start = pos + 1;
    else
      end = pos - 1;
  }

  if (next)
    return Math.max(start, end);
  else
    return Math.min(start, end);
};
