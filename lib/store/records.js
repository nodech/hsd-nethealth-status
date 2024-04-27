/*!
 * records.js - Generic records.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const bufio = require('bufio');

class Uint32Record extends bufio.Struct {
  constructor(value = 0) {
    super();

    this._value = 0;

    this.value = value;
  }

  get value() {
    return this._value;
  }

  set value(value) {
    assert(value >>> 0 === value);
    this._value = value;
  }

  getSize() {
    return 4;
  }

  write(bw) {
    bw.writeU32(this.value);
    return this;
  }

  read(br) {
    this.value = br.readU32();
    return this;
  }
}

class Uint64Record extends bufio.Struct {
  constructor(value = 0) {
    super();
    this._value = value;
  }

  get value() {
    return this._value;
  }

  set value(value) {
    assert(Number.isSafeInteger(value));
    this._value = value;
  }

  write(bw) {
    bw.writeU64(this.value);
    return this;
  }

  read(br) {
    this.value = br.readU64();
    return this;
  }
}

class TimestampRecord extends Uint64Record {
  constructor(timestamp = 0) {
    super(timestamp);
  }

  get timestamp() {
    return this.value;
  }
}

class TotalOnlineRecord extends Uint32Record {
  constructor(value = 0) {
    super(value);
  }

  get total() {
    return this.value;
  }
}

exports.Uint32Record = Uint32Record;
exports.Uint64Record = Uint64Record;
exports.TimestampRecord = TimestampRecord;
exports.TotalOnlineRecord = TotalOnlineRecord;
