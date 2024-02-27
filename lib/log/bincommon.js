/*!
 * bincommon.js - Common utilities for json writer and json reader.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const binet = require('binet');
const bufio = require('bufio');

const {error2code, code2error} = require('./common');

const common = exports;

common.FILE_FORMAT = 'event-%d.bin1%s';
common.FILE_REGEX = /^event-(?<ts>\d+)\.bin1(?<gz>\.gz)?$/;
common.STORE_NAME = 'events';
common.EXT = 'bin1';

common.fileOptions = {
  regex: common.FILE_REGEX,
  format: common.FILE_FORMAT,
  name: common.STORE_NAME,
  ext: common.EXT
};

common.ZERO_KEY = Buffer.alloc(33, 0x00);

common.PacketTypes = {
  CONFIG: 0,
  ENTRY: 1
};

class ConfigEntry extends bufio.Struct {
  constructor() {
    super();
    this.frequency = 0;
    this.interval = 0;
  }

  fromJSON(json) {
    this.frequency = json.frequency;
    this.interval = json.interval;
    return this;
  }

  toJSON() {
    return {
      frequency: this.frequency,
      interval: this.interval
    };
  }

  write(bw) {
    bw.writeU64(this.frequency);
    bw.writeU64(this.interval);
    return this;
  }

  read(br) {
    this.frequency = br.readU64();
    this.interval = br.readU64();
    return this;
  }

  size() {
    return 8 + 8;
  }
}

class Result extends bufio.Struct {
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
  }

  fromJSON(json) {
    this.peerVersion = json.peer.version;
    this.services = json.peer.services;
    this.height = json.peer.height;
    this.agent = json.peer.agent;
    this.noRelay = json.peer.noRelay;
    this.brontide = json.peer.brontide;

    this.pruned = json.chain.pruned;
    this.treeCompacted = json.chain.treeCompacted;
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

  write(bw) {
    bw.writeVarint2(this.peerVersion);
    bw.writeVarint2(this.services);
    bw.writeVarint2(this.height);
    bw.writeU8(this.agent.length);
    bw.writeString(this.agent, 'ascii');

    let details = 0;

    if (this.noRelay)
      details |= 1;

    if (this.brontide)
      details |= 2;

    if (this.pruned)
      details |= 4;

    if (this.treeCompacted)
      details |= 8;

    bw.writeU8(details);

    return this;
  }

  read(br) {
    this.peerVersion = br.readVarint2();
    this.services = br.readVarint2();
    this.height = br.readVarint2();
    const agentSize = br.readU8();
    this.agent = br.readString(agentSize, 'ascii');

    const details = br.readU8();

    this.noRelay = (details & 1) !== 0;
    this.brontide = (details & 2) !== 0;
    this.pruned = (details & 4) !== 0;
    this.treeCompacted = (details & 8) !== 0;

    return this;
  }

  size() {
    let size = 0;
    size += bufio.sizeVarint2(this.peerVersion);
    size += bufio.sizeVarint2(this.services);
    size += bufio.sizeVarint2(this.height);
    size += this.agent.length;
    size += 2;
    return size;
  }
}

class Entry extends bufio.Struct {
  constructor() {
    super();

    this.time = 0;
    this.host = '0.0.0.0';
    this.port = 0;
    this.key = common.ZERO_KEY;
    this.error = null;
    this.errorCode = null;
    this.result = null;
  }

  get hostname() {
    return binet.toHostname(this.host, this.port, this.key);
  }

  get isIPv4() {
    return binet.isIPv4(binet.decode(this.host));
  }

  get hasKey() {
    return !this.key.equals(common.ZERO_KEY);
  }

  fromJSON(json) {
    this.time = json.time;
    this.host = json.host;
    this.port = json.port;
    this.key = Buffer.from(json.key, 'hex');
    this.error = json.error;

    if (this.error) {
      try {
        this.errorCode = error2code(this.error);
      } catch (e) {
        console.error(e);
      }
    }

    if (json.result)
      this.result = new Result().fromJSON(json.result);

    return this;
  }

  toJSON(config) {
    return {
      time: this.time,
      hostname: this.hostname,
      host: this.host,
      port: this.port,
      key: this.key.toString('hex'),
      error: this.error,
      result: this.result ? this.result.toJSON() : null,
      frequency: config.frequency,
      interval: config.interval
    };
  }

  write(bw, logTimestamp) {
    let details = 0;

    if (this.isIPv4)
      details |= 1;

    if (this.hasKey)
      details |= 2;

    if (this.error)
      details |= 4;

    if (this.error && this.errorCode)
      details |= 8;

    const timeDiff = logTimestamp - this.time;

    bw.writeVarint2(timeDiff);
    bw.writeU8(details);

    if (this.isIPv4)
      binet.writeBW(bw, this.host, 4);
    else
      binet.writeBW(bw, this.host, 16);

    bw.writeU16(this.port);

    if (this.hasKey)
      bw.writeBytes(this.key);

    if (this.error && !this.errorCode) {
      const buffer = Buffer.from(this.error, 'utf8');
      bw.writeVarint2(buffer.length);
      bw.writeBytes(buffer);
    } else if (this.error && this.errorCode) {
      bw.writeU8(this.errorCode);
    }

    if (this.result)
      this.result.write(bw);

    return this;
  }

  read(br, logTimestamp) {
    this.time = logTimestamp - br.readVarint2();

    const details = br.readU8();

    const isIPv4 = (details & 1) !== 0;
    const hasKey = (details & 2) !== 0;
    const hasError = (details & 4) !== 0;
    const hasErrorCode = (details & 8) !== 0;

    if (isIPv4)
      this.host = binet.readBR(br, 4);
    else
      this.host = binet.readBR(br, 16);

    this.port = br.readU16();

    if (hasKey)
      this.key = br.readBytes(33);

    if (hasError && !hasErrorCode) {
      const size = br.readVarint2();
      this.error = br.readString(size, 'utf8');
    } else if (hasError && hasErrorCode) {
      this.errorCode = br.readU8();
      try {
        this.error = code2error(this.errorCode);
      } catch (e) {
        console.log(logTimestamp, this);
        throw e;
      }
    }

    if (!hasError) {
      this.result = new Result();
      this.result.read(br);
    }

    return this;
  }

  size(logTimestamp) {
    let size = 0;
    const diff = logTimestamp - this.time;
    size += bufio.sizeVarint2(diff);
    size += 1;

    if (this.isIPv4)
      size += 4;
    else
      size += 16;

    size += 2;

    if (this.hasKey)
      size += 33;

    if (this.error && !this.errorCode) {
      const bufLen = Buffer.byteLength(this.error, 'utf8');
      size += bufio.sizeVarint2(bufLen);
      size += bufLen;
    } else if (this.error && this.errorCode) {
      size += 1;
    }

    if (this.result)
      size += this.result.size();

    return size;
  }
}

common.ConfigEntry = ConfigEntry;
common.Result = Result;
common.Entry = Entry;
