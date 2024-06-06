/*!
 * entry.js - Entries.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const assert = require('bsert');
const bufio = require('bufio');
const binet = require('binet');
const netCommon = require('hsd/lib/net/common');

const ZERO_PUB = Buffer.alloc(33, 0x00).toString('hex');
const VERSION_REGEX = /^\/hsd:(\d{1,2}\.\d{1,2}\.\d{1,2})\/.*$/;

class DNSEntry extends bufio.Struct {
  constructor() {
    super();
    this.logTimestamp = 0;

    this.time = 0;
    this.hostname = '';

    this.error = null;
    this.result = null;

    this.frequency = 0;
    this.interval = 0;
  }

  get key() {
    return Buffer.from(this.hostname, 'ascii');
  }

  get duration() {
    return this.logTimestamp - this.time;
  }

  isSuccessful() {
    return this.error == null && this.result != null;
  }

  isFailed() {
    return this.error != null;
  }

  getSize() {
    let size = 0;

    size += 16;
    size += bufio.sizeVarString(this.hostname, 'ascii');
    size += 1;

    if (this.result != null) {
      size += bufio.sizeVarString(this.result);
    }

    if (this.error != null) {
      size += bufio.sizeVarString(this.error);
    }

    size += 16;

    return size;
  }

  write(bw) {
    bw.writeU64(this.logTimestamp);

    bw.writeU64(this.time);
    bw.writeVarString(this.hostname, 'ascii');

    let flags = 0;

    if (this.result != null)
      flags |= 1;

    if (this.error != null)
      flags |= 2;

    bw.writeU8(flags);

    if (this.result != null)
      bw.writeVarString(this.result);

    if (this.error != null)
      bw.writeVarString(this.error);

    bw.writeU64(this.frequency);
    bw.writeU64(this.interval);

    return bw;
  }

  read(br) {
    this.logTimestamp = br.readU64();

    this.time = br.readU64();
    this.hostname = br.readVarString('ascii');

    const flags = br.readU8();

    if (flags & 1)
      this.result = br.readVarString();

    if (flags & 2)
      this.error = br.readVarString();

    this.frequency = br.readU64();
    this.interval = br.readU64();

    return this;
  }

  toJSON() {
    return {
      logTimestamp: this.logTimestamp,

      info: {
        time: this.time,
        hostname: this.hostname,

        error: this.error,
        result: this.result,

        frequency: this.frequency,
        interval: this.interval
      }
    };
  }

  fromJSON(json) {
    this.logTimestamp = json.logTimestamp;

    const {info} = json;

    this.time = info.time;
    this.hostname = info.hostname;

    if (info.error && typeof info.error !== 'string') {
      info.error = info.error.code || 'unknown error.';
    }

    this.error = info.error;
    this.result = info.result;

    assert(!this.error || !this.result, 'Cannot have both error and result.');

    this.frequency = info.frequency;
    this.interval = info.interval;
  }

  static fromJSON(json) {
    const entry = new this();
    entry.fromJSON(json);
    return entry;
  }
}

class NodeDetails extends bufio.Struct {
  constructor() {
    super();

    this.peerVersion = 0;
    this.services = 0;
    this.height = 0;
    this.agent = '';
    this.noRelay = false;
    this.brontide = false;

    this.pruned = false;
    this.treeCompacted = false;

    this._version = null;
  }

  get version() {
    if (this._version)
      return this._version;

    const match = this.agent.match(VERSION_REGEX);

    if (!match) {
      this._version = 'other';
      return this._version;
    }

    this._version = match[1];
    return this._version;
  }

  canSync() {
    return (this.services & netCommon.services.NETWORK) && !this.noRelay;
  }

  hasBloom() {
    return this.services & netCommon.services.BLOOM;
  }

  getSize() {
    let size = 0;

    size += 4 + 4 + 4;
    size += bufio.sizeVarString(this.agent, 'ascii');
    size += 1;

    return size;
  }

  write(bw) {
    bw.writeU32(this.peerVersion);
    bw.writeU32(this.services);
    bw.writeU32(this.height);
    bw.writeVarString(this.agent, 'ascii');

    let flags = 0;

    if (this.noRelay)
      flags |= 1;

    if (this.brontide)
      flags |= 2;

    if (this.pruned)
      flags |= 4;

    if (this.treeCompacted)
      flags |= 8;

    bw.writeU8(flags);

    return bw;
  }

  read(br) {
    this.peerVersion = br.readU32();
    this.services = br.readU32();
    this.height = br.readU32();
    this.agent = br.readVarString('ascii');

    const flags = br.readU8();

    this.noRelay = (flags & 1) !== 0;
    this.brontide = (flags & 2) !== 0;
    this.pruned = (flags & 4) !== 0;
    this.treeCompacted = (flags & 8) !== 0;

    return this;
  }

  fromJSON(json) {
    const {peer, chain} = json;

    this.peerVersion = peer.version;
    this.services = peer.services;
    this.height = peer.height;
    this.agent = peer.agent;
    this.noRelay = peer.noRelay;
    this.brontide = peer.brontide;

    this.pruned = chain.pruned;
    this.treeCompacted = chain.treeCompacted;

    return this;
  }

  toJSON() {
    return {
      peer: {
        version: this.peerVersion,
        services: this.services,
        height: this.height,
        agent: this.agent,
        noRelay: this.noRelay,
        brontide: this.brontide
      },
      chain: {
        pruned: this.pruned,
        treeCompacted: this.treeCompacted
      }
    };
  }

  static fromJSON(json) {
    const details = new this();
    details.fromJSON(json);
    return details;
  }
}

class NodeEntry extends bufio.Struct {
  /** @type {NodeDetails?} */
  result;

  /** @type {String?} */
  error;

  constructor() {
    super();

    this.logTimestamp = 0;

    this.time = 0;
    this.host = '';
    this.port = 0;
    this.brontide = false;

    this.error = null;
    this.result = null;

    this.frequency = 0;
    this.interval = 0;

    this._rawHost = null;
    this._rawHostAndPort = null;
  }

  get key() {
    return this.rawHostAndPort;
  }

  /**
   * @returns {Buffer}
   */

  get rawHost() {
    if (this._rawHost)
      return this._rawHost;

    this._rawHost = binet.decode(this.host);
    return this._rawHost;
  }

  /**
   * @param {Buffer} raw
   */

  set rawHost(raw) {
    this.host = binet.encode(raw);
    this._rawHost = raw;
  }

  /**
   * @returns {Buffer}
   */

  get rawHostAndPort() {
    if (this._rawHostAndPort)
      return this._rawHostAndPort;

    const raw = Buffer.alloc(16 + 2);

    this.rawHost.copy(raw, 0);
    raw.writeUInt16BE(this.port, 16);

    this._rawHostAndPort = raw;
    return this._rawHostAndPort;
  }

  /**
   * @param {Buffer} raw
   */

  set rawHostAndPort(raw) {
    this.rawHost = raw.slice(0, 16);
    this.port = raw.readUInt16BE(16);
    this._rawHostAndPort = raw;
  }

  get duration() {
    return this.logTimestamp - this.time;
  }

  isSuccessful() {
    return this.error == null && this.result != null;
  }

  isFailed() {
    return this.error != null;
  }

  getSize() {
    let size = 0;

    size += 16;
    size += 18;

    size += 1;

    if (this.error)
      size += bufio.sizeVarString(this.error);

    if (this.result)
      size += this.result.getSize();

    size += 16;

    return size;
  }

  write(bw) {
    bw.writeU64(this.logTimestamp);

    bw.writeU64(this.time);
    bw.writeBytes(this.rawHostAndPort);

    let flags = 0;

    flags |= this.brontide ? 1 : 0;
    flags |= this.error ? 2 : 0;
    flags |= this.result ? 4 : 0;

    bw.writeU8(flags);

    if (this.error)
      bw.writeVarString(this.error);

    if (this.result)
      this.result.write(bw);

    bw.writeU64(this.frequency);
    bw.writeU64(this.interval);

    return bw;
  }

  read(br) {
    this.logTimestamp = br.readU64();

    this.time = br.readU64();
    this.rawHostAndPort = br.readBytes(18);

    const flags = br.readU8();

    this.brontide = (flags & 1) !== 0;

    if (flags & 2)
      this.error = br.readVarString();

    if (flags & 4)
      this.result = NodeDetails.read(br);

    this.frequency = br.readU64();
    this.interval = br.readU64();

    return this;
  }

  toJSON() {
    return {
      logTimestamp: this.logTimestamp,

      info: {
        time: this.time,
        host: this.host,
        port: this.port,
        brontide: this.brontide,

        error: this.error,
        result: this.result?.toJSON(),

        frequency: this.frequency,
        interval: this.interval
      }
    };
  }

  fromJSON(json) {
    this.logTimestamp = json.logTimestamp;

    const {info} = json;

    this.time = info.time;
    this.host = info.host;
    this.port = info.port;
    this.brontide = info.brontide;

    this.error = info.error;

    if (info.result)
      this.result = NodeDetails.fromJSON(info.result);

    assert(!this.error || !this.result, 'Cannot have both error and result.');

    this.frequency = info.frequency;
    this.interval = info.interval;

    return this;
  }

  fromLogJSON(json) {
    this.logTimestamp = json.logTimestamp;

    const {info} = json;

    this.time = info.time;
    this.host = info.host;
    this.port = info.port;
    this.brontide = info.key !== ZERO_PUB;

    this.error = info.error;

    if (info.result)
      this.result = NodeDetails.fromJSON(info.result);

    assert(!this.error || !this.result, 'Cannot have both error and result.');

    this.frequency = info.frequency;
    this.interval = info.interval;

    return this;
  }

  static fromJSON(json) {
    const entry = new this();
    entry.fromJSON(json);
    return entry;
  }

  static fromLogJSON(json) {
    const entry = new this();
    entry.fromLogJSON(json);
    return entry;
  }
}

exports.DNSEntry = DNSEntry;
exports.NodeEntry = NodeEntry;
exports.NodeDetails = NodeDetails;
