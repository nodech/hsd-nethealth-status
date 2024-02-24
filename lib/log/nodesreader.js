/*!
 * binreader.js - Read stored files event by event.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const bufio = require('bufio');
const Reader = require('./reader');
const {StoreOptions} = require('./common');
const {
  fileOptions,
  PacketTypes,
  ConfigEntry,
  Entry
} = require('./bincommon');

const YEARS_20_MS = 631152000000;

class JSONReader extends Reader {
  constructor(options) {
    super(new StoreOptions(options), fileOptions);
  }

  async *[Symbol.asyncIterator]() {
    assert(this.stream);

    let hasNext;
    let lastTimestamp = 0;
    let lastConfig = null;

    do {
      hasNext = false;

      let left = Buffer.alloc(0);
      for await (const data of this.stream) {
        if (data.length === 0)
          continue;

        const buf = Buffer.concat([left, data]);
        const br = bufio.read(buf, true);

        chunk: while (br.left() > 0) {
          const offset = br.offset;
          const type = br.readU8();
          const reset = () => {
            br.offset = offset;
            left = br.readBytes(br.left(), true);
          };

          const next = () => {
            left = left.slice(br.offset);
          };

          switch (type) {
            case PacketTypes.CONFIG: {
              if (br.left() < 16) {
                reset();
                break chunk;
              }

              const config = ConfigEntry.read(br);
              lastConfig = config;
              next();
              break;
            }

            case PacketTypes.ENTRY: {
              if (br.left() < 2) {
                reset();
                break chunk;
              }

              const size = br.readU16();

              if (br.left() < size) {
                reset();
                break chunk;
              }

              const time = br.readVarint2();

              if (time > YEARS_20_MS)
                lastTimestamp = time;
              else
                lastTimestamp += time;

              const entry = Entry.read(br, lastTimestamp);

              const result = {
                logTimestamp: lastTimestamp,
                info: entry.toJSON(lastConfig)
              };

              yield result;
              next();
              break;
            }
            default: {
              throw new Error(`Unknown packet type: ${type}`);
            }
          }
        }
      }

      assert(left.length === 0);

      this.lastReadTimestamp = lastTimestamp;
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
