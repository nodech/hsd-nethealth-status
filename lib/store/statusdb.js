/*!
 * statusdb.js - Statuses database for handshake nethealth.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const path = require('node:path');
const os = require('node:os');
const EventEmitter = require('node:events');
const bfs = require('bfile');
const bdb = require('bdb');
const DNSIndexer = require('./dns');
const NodeIndexer = require('./node');

const {statusDB} = require('./layout');

const {
  STORE_NAME
} = require('./common');

class StatusDB extends EventEmitter {
  constructor(options) {
    super();

    this.options = new StatusDBOptions(options);
    this.db = bdb.create(this.options);

    this.version = 1;
    this.name = 'statusdb';

    this.dnsIndexer = new DNSIndexer(this);
    this.nodeIndexer = new NodeIndexer(this);
  }

  /**
   * Open database.
   * @returns {Promise}
   */

  async open() {
    await this.ensure();
    await this.db.open();

    await this.db.verify(statusDB.VERSION.encode(), this.name, this.version);
  }

  /**
   * Close database.
   * @returns {Promise}
   */

  async close() {
    await this.db.close();
  }

  /**
   * Ensure prefix directory exists.
   * @returns {Promise}
   */

  async ensure() {
    if (this.options.memory)
      return;

    if (!await bfs.exists(this.options.prefix))
      await bfs.mkdirp(this.options.prefix, 0o755);
  }

  /**
   * Index DNS entry.
   * @param {DNSEntry} entry
   * @returns {Promise}
   */

  async indexDNS(entry) {
    const batch = this.db.batch();
    await this.dnsIndexer.index(batch, entry);
    return batch.write();
  }

  /**
   * Index Node entry.
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexNode(entry) {
    const batch = this.db.batch();
    await this.nodeIndexer.index(batch, entry);
    return batch.write();
  }
}

class StatusDBOptions {
  constructor(options) {
    this.prefix = path.join(os.tmpdir(), 'hsd-nethealth');
    this.location = path.join(this.prefix, STORE_NAME);
    this.memory = false;

    this.set(options);
  }

  set(options) {
    if (options.prefix != null) {
      assert(typeof options.prefix === 'string');
      this.prefix = options.prefix;
      this.location = path.join(this.prefix, STORE_NAME);
    }

    if (options.location != null) {
      assert(typeof options.location === 'string');
      this.location = options.location;
    }

    if (options.memory != null) {
      assert(typeof options.memory === 'boolean');
      this.memory = options.memory;
    }

    return this;
  }
}

/*
 * Expose
 */

module.exports = StatusDB;
