/*!
 * node.js - Node log index for StatusDB.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

const assert = require('bsert');
const binet = require('binet');
const {BufferMap} = require('buffer-map');
const LRU = require('blru');
const {NodeEntry} = require('../entry');
const {nodes} =  require('./layout');
const {TimestampRecord} = require('./records');
const {UpCounts, NodeBucketStatus} = require('./noderecords');
const common = require('../common');

class NodeIndexer {
  constructor(sdb, options = {}) {
    this.sdb = sdb;
    this.db = sdb.db;
    this.options = options;
    this.bucket = this.db.bucket(nodes.prefix.encode());

    this.onlinePercentile = options.onlinePercentile || 0.90;

    this.cachesEnabled = true;
    this.lastUpCache = new LRU(10000, null, BufferMap);
    this.upCounts = null;
    this.isUpCache = new BufferMap();

    this.hourlyCacheEnabled = true;
    this.statusByHourByHostCache = new LRU(10000, null, BufferMap);
    this.upCountsHourlyCache = new LRU(10000);

    this.dailyCacheEnabled = true;
    this.statusDayByHostCache = new LRU(10000, null, BufferMap);
    this.upCountsDayCache = new LRU(10000);
  }

  /**
   * Index Node Entry
   * @param {Batch} batch
   * @param {NodeEntry} entry
   */

  async index(batch, entry) {
    assert(entry instanceof NodeEntry);

    batch = batch.bucket(nodes.prefix.encode());
    const now = Date.now();
    const dayAgo = now - common.DAY;
    const weeksAgo2 = now - 2 * common.WEEK;

    await this.indexTimestamp(batch, entry);
    await this.indexPort(batch, entry);

    if (entry.logTimestamp > dayAgo) {
      await this.indexUpCounts(batch, entry);
      // order matters for cache.
      await this.indexUp(batch, entry);
    }

    // order matters for cache, indexUpCount uses LastUp.
    await this.indexLastUp(batch, entry);
    await this.indexLastStatus(batch, entry);

    if (entry.logTimestamp > weeksAgo2) {
      await this.indexHourly(batch, entry);
    }

    await this.indexDaily(batch, entry);

    // await this.cleanup10mUpCounts(batch, entry);
    // ...

    await batch.write();
  }

  /**
   * Index last timestamp, pointer to the log.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   */

  async indexTimestamp(batch, entry) {
    const ts = new TimestampRecord(entry.logTimestamp);
    batch.put(nodes.LAST_TIMESTAMP.encode(), ts.encode());
  }

  /**
   * Get last timestamp, pointer to the log + 1.
   * @returns {Promise<Number>}
   */

  async getLastTimestamp() {
    const data = await this.bucket.get(nodes.LAST_TIMESTAMP.encode());

    if (!data)
      return 0;

    return TimestampRecord.decode(data).timestamp + 1;
  }

  /**
   * Index port mappings for the host.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexPort(batch, entry) {
    batch.put(nodes.PORT_MAPPINGS.encode(entry.rawHost, entry.port));
  }

  /**
   * Get port entries for a hostname.
   * @param {String} host
   * @yields {Promise<Number>}
   * @returns {AsyncGenerator<Number>}
   */

  async *getPorts(host) {
    const rawHost = binet.decode(host);

    const iter = this.bucket.iterator({
      gte: nodes.PORT_MAPPINGS.encode(rawHost, 0),
      lte: nodes.PORT_MAPPINGS.encode(rawHost, 0xffff),
      values: false
    });

    for await (const {key} of iter) {
      const [, port] = nodes.PORT_MAPPINGS.decode(key);
      yield port;
    }
  }

  /*
   * Last seen related methods.
   */

  /**
   * Index last seen status.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   */

  async indexLastUp(batch, entry) {
    if (entry.isSuccessful()) {
      if (this.cachesEnabled)
        this.lastUpCache.set(entry.key, entry);
      batch.put(nodes.LAST_UP.encode(entry.key), entry.encode());
    }
  }

  /**
   * Get last seen timestamp.
   * @param {Buffer} key
   * @returns {Promise<NodeEntry?>}
   */

  async getLastUp(key) {
    assert(Buffer.isBuffer(key));

    if (this.cachesEnabled && this.lastUpCache.has(key))
      return this.lastUpCache.get(key);

    const data = await this.bucket.get(nodes.LAST_UP.encode(key));

    if (!data)
      return null;

    return NodeEntry.decode(data);
  }

  /**
   * Index last status.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   */

  async indexLastStatus(batch, entry) {
    const encoded = entry.encode();

    const min10 = common.floorTime(entry.time, 10 * common.MINUTE);
    batch.put(nodes.LAST_STATUS.encode(entry.key), encoded);

    batch.put(nodes.STATUS_10_BY_HOST.encode(entry.key, min10), encoded);
  }

  /**
   * Get last status.
   * @param {Buffer} key
   * @returns {Promise<NodeEntry?>}
   */

  async getLastStatus(key) {
    assert(Buffer.isBuffer(key));

    const data = await this.bucket.get(nodes.LAST_STATUS.encode(key));

    if (!data)
      return null;

    return NodeEntry.decode(data);
  }

  /**
   * Get all hostports.
   * @generator
   * @yields {Promise<Buffer>}
   * @returns {AsyncGenerator<Buffer>}
   */

  async *getHostPorts() {
    const iter = this.bucket.iterator({
      gte: nodes.LAST_STATUS.min(),
      lte: nodes.LAST_STATUS.max(),
      values: false
    });

    for await (const {key} of iter) {
      yield nodes.LAST_STATUS.decode(key)[0];
    }
  }

  /**
   * Get last statuses by time.
   * @param {Buffer} hostport
   * @param {Number} since
   * @yield {Promise<[Number, NodeEntry]>}
   * @returns {AsyncGenerator<[Number, NodeEntry]>}
   */

  async *getLastStatusesByTime(hostport, since) {
    const iter = this.bucket.iterator({
      gte: nodes.STATUS_10_BY_HOST.encode(hostport, since),
      lte: nodes.STATUS_10_BY_HOST.max(hostport),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.STATUS_10_BY_HOST.decode(key)[1];
      yield [time, NodeEntry.decode(value)];
    }
  }

  /**
   * Online only.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexUp(batch, entry) {
    if (entry.isSuccessful()) {
      if (this.cachesEnabled)
        this.isUpCache.set(entry.key, true);

      batch.put(nodes.UP.encode(entry.key), null);
      return;
    }

    if (entry.isFailed()) {
      if (this.cachesEnabled)
        this.isUpCache.set(entry.key, false);
      batch.del(nodes.UP.encode(entry.key));
    }
  }

  /**
   * Get Online counts.
   * @returns
   */

  /**
   * Is Online
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async isUp(key) {
    assert(Buffer.isBuffer(key));

    if (this.cachesEnabled && this.isUpCache.has(key))
      return this.isUpCache.get(key);

    return this.bucket.has(nodes.UP.encode(key));
  }

  /**
   * Online counts.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexUpCounts(batch, entry) {
    const counts = await this.getUpCounts();
    const wasUp = await this.isUp(entry.key);
    const min10 = common.floorTime(entry.time, 10 * common.MINUTE);

    if (wasUp) {
      const oldStatus = await this.getLastUp(entry.key);
      assert(oldStatus);
      counts.sub(oldStatus);
    }

    if (entry.isSuccessful())
      counts.add(entry);

    const encodedCounts = counts.encode();
    batch.put(nodes.UP_COUNTS.encode(), encodedCounts);
    batch.put(nodes.UP_COUNTS_10.encode(min10), encodedCounts);

    if (this.cachesEnabled)
      this.upCounts = counts;
  }

  /**
   * Get online counts.
   * @returns {Promise<UpCounts>}
   */

  async getUpCounts() {
    if (this.cachesEnabled && this.upCounts)
      return this.upCounts;

    const data = await this.bucket.get(nodes.UP_COUNTS.encode());

    if (!data) {
      return new UpCounts();
    }

    return UpCounts.decode(data);
  }

  /**
   * Get 10 minute UP counts.
   * @param {Number} time
   * @returns {Promise<UpCounts>}
   */

  async getUpCounts10(time) {
    assert((time % (10 * common.MINUTE)) === 0);

    const data = await this.bucket.get(nodes.UP_COUNTS_10.encode(time));

    if (!data) {
      return new UpCounts();
    }

    return UpCounts.decode(data);
  }

  /**
   * Get 10 minute UP counts by time.
   * @param {Number} since
   * @yields {Promise<[Number, UpCounts]>}
   * @returns {AsyncGenerator<[Number, UpCounts]>}
   */

  async *get10mUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_10.encode(since),
      lte: nodes.UP_COUNTS_10.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.UP_COUNTS_10.decode(key)[0];
      yield [time, UpCounts.decode(value)];
    }
  }

  /**
   * Clean up 10 minute UP counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanup10mUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_10.min(),
      lt: nodes.UP_COUNTS_10.encode(before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Get Online hostports.
   * @generator
   * @yields {Promise<Buffer>}
   * @returns {AsyncGenerator<Buffer>}
   */

  async *getOnlineHostPorts() {
    const iter = this.bucket.iterator({
      gte: nodes.UP.min(),
      lte: nodes.UP.max(),
      values: false
    });

    for await (const {key} of iter) {
      yield nodes.UP.decode(key)[0];
    }
  }

  /**
   * Index hourly status.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexHourly(batch, entry) {
    const min60 = common.floorTime(entry.time, common.HOUR);
    const bucket60key = nodes.STATUS_HOUR_BY_HOST.encode(entry.key, min60);
    const status60 = await this.getHourlyStatusByHost(bucket60key);

    const oldVirtEntry = status60.getVirtualEntry(
      entry, this.onlinePercentile);

    // new status
    status60.add(entry);
    const encoded60 = status60.encode();
    batch.put(bucket60key, encoded60);

    if (this.hourlyCacheEnabled)
      this.statusByHourByHostCache.set(bucket60key, status60);

    const newVirtEntry = status60.getVirtualEntry(
      entry, this.onlinePercentile);

    const counts = await this.getHourlyUpCounts(min60);

    if (oldVirtEntry.isSuccessful())
      counts.sub(oldVirtEntry);

    if (newVirtEntry.isSuccessful())
      counts.add(newVirtEntry);

    batch.put(nodes.UP_COUNTS_HOUR.encode(min60), counts.encode());
    if (this.hourlyCacheEnabled)
      this.upCountsHourlyCache.set(min60, counts);
  }

  /**
   * Get Hourly UpCounts.
   * @param {Number} time
   * @returns {Promise<UpCounts>}
   */

  async getHourlyUpCounts(time) {
    assert((time % common.HOUR) === 0);

    if (this.hourlyCacheEnabled && this.upCountsHourlyCache.has(time))
      return this.upCountsHourlyCache.get(time);

    const data = await this.bucket.get(nodes.UP_COUNTS_HOUR.encode(time));

    if (!data) {
      return new UpCounts();
    }

    return UpCounts.decode(data);
  }

  /**
   * Get hourly status by host
   * @param {Buffer} bucketKey
   * @returns {Promise<NodeBucketStatus>}
   */

  async getHourlyStatusByHost(bucketKey) {
    if (this.hourlyCacheEnabled && this.statusByHourByHostCache.has(bucketKey))
      return this.statusByHourByHostCache.get(bucketKey);

    const data = await this.bucket.get(bucketKey);

    if (!data) {
      return new NodeBucketStatus(common.HOUR);
    }

    return NodeBucketStatus.decode(data, common.HOUR);
  }

  /**
   * Get hourly statuses by time.
   * @param {Buffer} key
   * @param {Number} since
   * @yields {Promise<[Number, NodeBucketStatus]>}
   * @returns {AsyncGenerator<[Number, NodeBucketStatus]>}
   */

  async *getHourlyStatusesByTime(key, since) {
    const iter = this.bucket.iterator({
      gte: nodes.STATUS_HOUR_BY_HOST.encode(key, since),
      lte: nodes.STATUS_HOUR_BY_HOST.max(key),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.STATUS_HOUR_BY_HOST.decode(key)[1];
      yield [time, NodeBucketStatus.decode(value, common.HOUR)];
    }
  }

  /**
   * Get hourly up counts by time.
   * @param {Number} since
   * @yields {Promise<[Number, UpCounts]>}
   * @returns {AsyncGenerator<[Number, UpCounts]>}
   */

  async *getHourlyUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_HOUR.encode(since),
      lte: nodes.UP_COUNTS_HOUR.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.UP_COUNTS_HOUR.decode(key)[0];
      yield [time, UpCounts.decode(value)];
    }
  }

  /**
   * Clean up hourly up counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupHourlyUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_HOUR.min(),
      lt: nodes.UP_COUNTS_HOUR.encode(before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Index daily status.
   * @param {Batch} batch
   * @param {NodeEntry} entry
   * @returns {Promise}
   */

  async indexDaily(batch, entry) {
    const minDay = common.floorTime(entry.time, common.DAY);
    const statusDay = await this.getDailyStatusByHost(entry.key, minDay);

    const oldVirtEntry = statusDay.getVirtualEntry(
      entry, this.onlinePercentile);

    statusDay.add(entry);
    const encodedDay = statusDay.encode();
    const bucketDayKey = nodes.STATUS_DAY_BY_HOST.encode(entry.key, minDay);
    batch.put(bucketDayKey, encodedDay);

    if (this.dailyCacheEnabled)
      this.statusDayByHostCache.set(bucketDayKey, statusDay);

    const newVirtEntry = statusDay.getVirtualEntry(
      entry, this.onlinePercentile);

    const counts = await this.getDailyUpCounts(minDay);

    if (oldVirtEntry.isSuccessful())
      counts.sub(oldVirtEntry);

    if (newVirtEntry.isSuccessful())
      counts.add(newVirtEntry);

    if (this.dailyCacheEnabled)
      this.upCountsDayCache.set(minDay, counts);
    batch.put(nodes.UP_COUNTS_DAY.encode(minDay), counts.encode());
  }

  /**
   * Get daily status by host.
   * @param {Buffer} key
   * @param {Number} time
   * @returns {Promise<NodeBucketStatus>}
   */

  async getDailyStatusByHost(key, time) {
    const bucketKey = nodes.STATUS_DAY_BY_HOST.encode(key, time);

    if (this.dailyCacheEnabled && this.statusDayByHostCache.has(bucketKey))
      return this.statusDayByHostCache.get(bucketKey);

    const data = await this.bucket.get(bucketKey);

    if (!data) {
      return new NodeBucketStatus(common.DAY);
    }

    return NodeBucketStatus.decode(data, common.DAY);
  }

  /**
   * Get daily UpCounts.
   * @param {Number} time
   * @returns {Promise<UpCounts>}
   */

  async getDailyUpCounts(time) {
    assert((time % common.DAY) === 0);

    if (this.dailyCacheEnabled && this.upCountsDayCache.has(time))
      return this.upCountsDayCache.get(time);

    const data = await this.bucket.get(nodes.UP_COUNTS_DAY.encode(time));

    if (!data) {
      return new UpCounts();
    }

    return UpCounts.decode(data);
  }

  /**
   * Get daily statuses by time.
   * @param {Buffer} key
   * @param {Number} since
   * @yield {Promise<[Number, NodeBucketStatus]>}
   * @returns {AsyncGenerator<[Number, NodeBucketStatus]>}
   */

  async *getDailyStatusesByTime(key, since) {
    const iter = this.bucket.iterator({
      gte: nodes.STATUS_DAY_BY_HOST.encode(key, since),
      lte: nodes.STATUS_DAY_BY_HOST.max(key),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.STATUS_DAY_BY_HOST.decode(key)[1];
      yield [time, NodeBucketStatus.decode(value, common.DAY)];
    }
  }

  /**
   * Get daily UP counts by time.
   * @param {Number} since
   * @yield {Promise<[Number, UpCounts]>}
   * @returns {AsyncGenerator<[Number, UpCounts]>}
   */

  async *getDailyUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_DAY.encode(since),
      lte: nodes.UP_COUNTS_DAY.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = nodes.UP_COUNTS_DAY.decode(key)[0];
      yield [time, UpCounts.decode(value)];
    }
  }

  /**
   * Clean up daily up counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupDailyUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: nodes.UP_COUNTS_DAY.min(),
      lt: nodes.UP_COUNTS_DAY.encode(before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Clean up stale indexes.
   * @returns {Promise}
   */

  async cleanupStale() {
    const cleanUp = async (index) => {
      const iter = this.bucket.iterator({
        gte: index.min(),
        lte: index.max()
      });

      const batch = this.bucket.batch();
      for await (const {key} of iter)
        batch.del(key);
    };

    await cleanUp(nodes.STATUS_10_BY_TIME);
    await cleanUp(nodes.STATUS_HOUR_BY_TIME);
    await cleanUp(nodes.STATUS_DAY_BY_TIME);
  }
}

/*
 * Expose
 */

module.exports = NodeIndexer;
