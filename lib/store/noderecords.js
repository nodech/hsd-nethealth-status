/*!
 * noderecords.js - Records for nodes.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const bufio = require('bufio');
const {NodeEntry, NodeDetails} = require('../entry');
const netCommon = require('hsd/lib/net/common');

class UpCounts extends bufio.Struct {
  constructor() {
    super();

    this.total = 0;
    this.spv = 0;
    this.compacted = 0;
    this.pruned = 0;
    this.canSync = 0;
    this.version = new Map();
  }

  /**
   * @param {NodeEntry} entry
   */

  add(entry) {
    assert(entry.isSuccessful());

    this.total += 1;

    const {result} = entry;

    this.spv += result.hasBloom() ? 1 : 0;
    this.compacted += result.treeCompacted ? 1 : 0;
    this.pruned += result.pruned ? 1 : 0;
    this.canSync += result.canSync() ? 1 : 0;

    const entryVersion = result.version;
    const version = this.version.get(entryVersion) || 0;
    this.version.set(entryVersion, version + 1);
  }

  /**
   * @param {NodeEntry} entry
   */

  sub(entry) {
    assert(entry.isSuccessful());

    this.total -= 1;

    const {result} = entry;

    this.spv -= result.hasBloom() ? 1 : 0;
    this.compacted -= result.treeCompacted ? 1 : 0;
    this.pruned -= result.pruned ? 1 : 0;
    this.canSync -= result.canSync() ? 1 : 0;

    const entryVersion = result.version;
    const version = this.version.get(entryVersion) || 0;
    assert(version > 0, 'Version count cannot be negative.');
    this.version.set(entryVersion, version - 1);

    if (version - 1 === 0)
      this.version.delete(entryVersion);
  }

  getSize() {
    let size = 5 * 4;

    size += 4;
    for (const [key] of this.version) {
      size += bufio.sizeVarString(key, 'ascii');
      size += 4;
    }

    return size;
  }

  write(bw) {
    bw.writeU32(this.total);
    bw.writeU32(this.spv);
    bw.writeU32(this.compacted);
    bw.writeU32(this.pruned);
    bw.writeU32(this.canSync);

    bw.writeU32(this.version.size);
    for (const [key, value] of this.version) {
      bw.writeVarString(key, 'ascii');
      bw.writeU32(value);
    }

    return bw;
  }

  read(br) {
    this.total = br.readU32();
    this.spv = br.readU32();
    this.compacted = br.readU32();
    this.pruned = br.readU32();
    this.canSync = br.readU32();

    const count = br.readU32();
    for (let i = 0; i < count; i++) {
      const key = br.readVarString('ascii');
      const value = br.readU32();
      this.version.set(key, value);
    }

    return this;
  }

  toJSON() {
    return {
      total: this.total,
      spv: this.spv,
      compacted: this.compacted,
      pruned: this.pruned,
      canSync: this.canSync,
      version: Object.fromEntries(this.version)
    };
  }
}

class NodeBucketStatus extends UpCounts {
  constructor(timeRange) {
    super();

    this.up = 0;
    this.timeRange = timeRange || 0;
  }

  get percentile() {
    return this.total === 0 ? -1 : this.up / this.total;
  }

  getVirtualEntry(entry, onPerc = 0.90, featurePerc = 0.5) {
    const virtEntry = new NodeEntry();
    const result = new NodeDetails();

    virtEntry.logTimestamp = 0;
    virtEntry.time = 0;
    virtEntry.host = entry.host;
    virtEntry.port = entry.port;
    virtEntry.brontide = entry.brontide;
    virtEntry.frequency = 0;
    virtEntry.interval = 0;

    virtEntry.error = null;
    virtEntry.result = null;

    if (this.percentile < onPerc) {
      virtEntry.error = 'Node is down';
      return virtEntry;
    }

    if (entry.isSuccessful()) {
      result.peerVersion = entry.result.peerVersion;
      result.height = entry.result.height;
    }

    if ((this.spv / this.total) > featurePerc) {
      result.services = this.services | netCommon.services.BLOOM;
    }

    if ((this.canSync / this.total) > featurePerc) {
      result.services |= netCommon.services.NETWORK;
      result.noRelay = false;
    }

    if ((this.compacted / this.total) > featurePerc) {
      result.treeCompacted = true;
    }

    if ((this.pruned / this.total) > featurePerc) {
      result.pruned = true;
    }

    const versions = Array.from(this.version.keys());

    versions.sort((a, b) => {
      return this.version.get(b) - this.version.get(a);
    });

    result.agent = `/hsd:${versions[0]}/`;

    virtEntry.result = result;

    return virtEntry;
  }

  /**
   * @param {NodeEntry} entry
   */

  add(entry) {
    if (entry.isFailed()) {
      this.total += 1;
      return;
    }

    this.up += 1;
    super.add(entry);
  }

  getSize() {
    return 4 + super.getSize();
  }

  write(bw) {
    bw.writeU32(this.up);
    return super.write(bw);
  }

  read(br) {
    this.up = br.readU32();
    return super.read(br);
  }

  static decode(data, timeRange) {
    return new this(timeRange).decode(data);
  }
}

exports.UpCounts = UpCounts;
exports.NodeBucketStatus = NodeBucketStatus;
