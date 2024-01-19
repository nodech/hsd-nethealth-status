/*!
 * utils.js - Utilities for JSONStoreWriter and JSONStoreReader
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const zlib = require('node:zlib');
const assert = require('bsert');
const bfs = require('bfile');

exports.closeStream = function closeStream(stream) {
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

exports.openStream = function openStream(filename, flags) {
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
};

exports.gzipFile = function gzipFile(file) {
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

exports.getFileSize = async function getFileSize(file) {
  try {
    const stat = await bfs.stat(file);

    return stat.size;
  } catch (e) {
    if (e.code === 'ENOENT')
      return 0;

    throw e;
  }
};
