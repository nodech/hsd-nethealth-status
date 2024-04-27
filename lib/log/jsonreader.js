/*!
 * jsonreader.js - Read stored files event by event.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const readline = require('node:readline');
const assert = require('bsert');
const Reader = require('./reader');
const {fileOptions} = require('./jsoncommon');
const {StoreOptions} = require('./common');

class JSONReader extends Reader {
  constructor(options) {
    super(new StoreOptions(options), fileOptions);
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

        if (json.logTimestamp < this.lastReadTimestamp)
          continue;

        this.lastReadTimestamp = json.logTimestamp;
        yield json;
      }

      const nextFileOpened = await this.openNextFile();

      if (nextFileOpened)
        hasNext = true;
    } while (hasNext);
  }
}

/*
 * Expose
 */

module.exports = JSONReader;
