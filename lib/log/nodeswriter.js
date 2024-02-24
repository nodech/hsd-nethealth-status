/*!
 * nodeswriter.js - Log events into binary files.
 * This is stateful implementation of a logger.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const Writer = require('./writer');
const bufio = require('bufio');
const {StoreOptions} = require('./common');
const {fileOptions} = require('./bincommon');

const {
  PacketTypes,
  ConfigEntry,
  Entry
} = require('./bincommon');

class NodesWriter extends Writer {
  constructor(options) {
    super(new StoreOptions(options), fileOptions);

    // State
    this.lastTimestamp = null;
    this.lastConfig = null;
  }

  _onClose() {
    this.reset();
  }

  reset() {
    this.lastTimestamp = null;
    this.lastConfig = null;
  }

  writeLog(timestamp, entry) {
    if (this.lastConfig == null) {
      this.writeConfig({
        frequency: entry.frequency,
        interval: entry.interval
      }, timestamp);
    }

    this.writeEntry(timestamp, entry);
  }

  writeConfig(options, timestamp) {
    const config = ConfigEntry.fromJSON(options);
    const packet = bufio.write(1 + config.size());
    packet.writeU8(PacketTypes.CONFIG);
    config.write(packet);
    this.write(packet.render(), timestamp);
    this.lastConfig = config;
  }

  writeEntry(timestamp, entryJSON) {
    let writeTime = timestamp;

    if (this.lastTimestamp != null)
      writeTime = timestamp - this.lastTimestamp;

    this.lastTimestamp = timestamp;

    const entry = new Entry();
    entry.fromJSON(entryJSON);

    const timeSize = bufio.sizeVarint2(writeTime);
    const entrySize = entry.size(timestamp);
    const packet = bufio.write(3 + timeSize + entrySize);

    packet.writeU8(PacketTypes.ENTRY);
    packet.writeU16(timeSize + entrySize);
    packet.writeVarint2(writeTime);
    entry.write(packet, timestamp);

    this.write(packet.render(), timestamp);
  }
}

module.exports = NodesWriter;
