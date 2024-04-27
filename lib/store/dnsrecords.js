/*!
 * dnsrecords.js - Simple serializations for DNS records.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const bufio = require('bufio');

class DNSBucketStatus extends bufio.Struct {
  /**
   * @param {Number} timeRange
   */

  constructor(timeRange) {
    super();

    this.up = 0;
    this.total = 0;
    this.timeRange = timeRange || 0;
  }

  /**
   * @returns {this}
  */

  clone() {
    const newStatus = new this.constructor(this.timeRange);
    newStatus.up = this.up;
    newStatus.total = this.total;

    return newStatus;
  }

  get percentage() {
    return this.total === 0 ? -1 : this.up / this.total;
  }

  getSize() {
    return 8;
  }

  write(bw) {
    bw.writeU32(this.up);
    bw.writeU32(this.total);

    return bw;
  }

  read(br) {
    this.up = br.readU32();
    this.total = br.readU32();
    return this;
  }

  toJSON() {
    return {
      up: this.up,
      total: this.total
    };
  }

  fromJSON(json) {
    this.up = json.up;
    this.total = json.total;
    return this;
  }

  static fromJSON(json, timeRange) {
    return new this(timeRange).fromJSON(json);
  }

  static decode(data, timeRange) {
    return new this(timeRange).decode(data);
  }
}

exports.DNSBucketStatus = DNSBucketStatus;
