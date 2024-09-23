/*!
 * dns.js - DNS Indexer for StatusDB.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const assert = require('bsert');
const {DNSEntry} = require('../entry');
const {dns} = require('./layout');
const {TotalOnlineRecord, TimestampRecord} = require('./records');
const {DNSBucketStatus} = require('./dnsrecords');
const common = require('../common');

class DNSIndexer {
  constructor(sdb, options = {}) {
    this.sdb = sdb;
    this.db = sdb.db;
    this.options = options;
    this.bucket = this.db.bucket(dns.prefix.encode());

    this.onlinePercentile = options.onlinePercentile || 0.90;
  }

  /**
   * Index DNS entry.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   * @returns {Promise}
   */

  async index(batch, entry) {
    assert(entry instanceof DNSEntry);

    batch = batch.bucket(dns.prefix.encode());

    await this.indexTimestamp(batch, entry);
    await this.indexLastUp(batch, entry);
    await this.indexLastStatus(batch, entry);
    await this.indexUp(batch, entry);

    await this.indexHourly(batch, entry);
    await this.indexDaily(batch, entry);

    await batch.write();
  }

  /*
   * General state management.
   */

  /**
   * Index last timestamp, pointer to the log.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   */

  async indexTimestamp(batch, entry) {
    const ts = new TimestampRecord(entry.logTimestamp);
    batch.put(dns.LAST_TIMESTAMP.encode(), ts.encode());
  }

  /**
   * Get last timestamp, pointer to the log + 1.
   * @returns {Promise<Number>}
   */

  async getLastTimestamp() {
    const data = await this.bucket.get(dns.LAST_TIMESTAMP.encode());

    if (!data)
      return 0;

    return TimestampRecord.decode(data).timestamp + 1;
  }

  /*
   * Last seen related methods.
   */

  /**
   * Index last seen status.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   */

  async indexLastUp(batch, entry) {
    if (entry.isSuccessful()) {
      const ts = new TimestampRecord(entry.time);
      batch.put(dns.LAST_UP.encode(entry.key), ts.encode());
    }
  }

  /**
   * Get last seen timestamp.
   * @param {Buffer} key
   * @returns {Promise<Number>}
   */

  async getLastUp(key) {
    assert(Buffer.isBuffer(key));

    const data = await this.bucket.get(dns.LAST_UP.encode(key));

    if (!data)
      return 0;

    return TimestampRecord.decode(data).timestamp;
  }

  /*
   * Last status related methods.
   */

  /**
   * Index last status.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   */

  async indexLastStatus(batch, entry) {
    const min10 = common.floorTime(entry.time, 10 * common.MINUTE);
    batch.put(dns.LAST_STATUS.encode(entry.key), entry.encode());

    batch.put(dns.STATUS_10_BY_HOST.encode(entry.key, min10), entry.encode());
  }

  /**
   * Get last status.
   * @param {Buffer} key
   * @returns {Promise<DNSEntry?>}
   */

  async getLastStatus(key) {
    assert(Buffer.isBuffer(key));

    const data = await this.bucket.get(dns.LAST_STATUS.encode(key));

    if (!data)
      return null;

    return DNSEntry.decode(data);
  }

  /**
   * Get all hostnames.
   * @generator
   * @yields {Promise<String>}
   * @returns {AsyncGenerator<String>}
   */

  async *getHostnames() {
    const iter = this.bucket.iterator({
      gte: dns.LAST_STATUS.min(),
      lte: dns.LAST_STATUS.max(),
      values: false
    });

    for await (const {key} of iter) {
      yield dns.LAST_STATUS.decode(key)[0].toString('utf8');
    }
  }

  /**
   * Get last statuses by time.
   * @param {String} hostname
   * @param {Number} since
   * @yields {Promise<[Number, DNSEntry]>}
   * @returns {AsyncGenerator<[Number, DNSEntry]>}
   */

  async *getLastStatusesByTime(hostname, since) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_10_BY_HOST.encode(hostkey, since),
      lte: dns.STATUS_10_BY_HOST.max(hostkey),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.STATUS_10_BY_HOST.decode(key)[1];
      yield [time, DNSEntry.decode(value)];
    }
  }

  /**
   * Clean up last statuses by time.
   * @param {String} hostname
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupLastStatusesByTime(hostname, before) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_10_BY_HOST.min(hostkey),
      lt: dns.STATUS_10_BY_HOST.encode(hostkey, before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /*
   * Online related methods.
   */

  /**
   * Online (Online only).
   * @param {Batch} batch
   * @param {DNSEntry} entry
   * @returns {Promise}
   */

  async indexUp(batch, entry) {
    const total = await this.getUpCount();
    const isUp = await this.isUp(entry.key);
    const oldCount = isUp ? 1 : 0;
    const newCount = entry.isSuccessful() ? 1 : 0;
    const newTotal = new TotalOnlineRecord(total - oldCount + newCount);
    const min10 = common.floorTime(entry.time, 10 * common.MINUTE);

    batch.put(dns.UP_COUNT.encode(), newTotal.encode());

    // TODO: Maybe clean up old records (e.g. 1 week old).
    batch.put(dns.UP_COUNT_10.encode(min10), newTotal.encode());

    if (entry.isSuccessful()) {
      batch.put(dns.UP.encode(entry.key), null);
      return;
    }

    if (entry.isFailed()) {
      batch.del(dns.UP.encode(entry.key));
      return;
    }
  }

  /**
   * Get Online count.
   * @returns {Promise<Number>}
   */

  async getUpCount() {
    const data = await this.bucket.get(dns.UP_COUNT.encode());

    if (!data)
      return 0;

    return TotalOnlineRecord.decode(data).total;
  }

  /**
   * is Online.
   * @param {Buffer} key
   * @returns {Promise<Boolean>}
   */

  async isUp(key) {
    assert(Buffer.isBuffer(key));

    const data = await this.bucket.get(dns.UP.encode(key));

    if (!data)
      return false;

    return true;
  }

  /**
   * Get 10 minute UP count.
   * @param {Number} time
   * @returns {Number}
   */

  async getUpCount10(time) {
    assert(time % (10 * common.MINUTE) === 0);
    const data = await this.bucket.get(dns.UP_COUNT_10.encode(time));

    if (!data)
      return 0;

    return TotalOnlineRecord.decode(data).total;
  }

  /**
   * Get 10 minute UP counts by time.
   * @param {Number} since
   * @yields {Promise<[Number, Number]>}
   * @returns {AsyncGenerator<[Number, Number]>}
   */

  async *get10mUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_10.encode(since),
      lte: dns.UP_COUNT_10.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.UP_COUNT_10.decode(key)[0];
      yield [time, TotalOnlineRecord.decode(value).total];
    }
  }

  /**
   * Clean up 10m up counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanup10mUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_10.min(),
      lt: dns.UP_COUNT_10.encode(before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Get online hosts.
   * @async
   * @generator
   * @yields {Promise<String>}
   * @returns {AsyncGenerator<String>}
   */

  async *getOnline() {
    const iter = this.bucket.iterator({
      gte: dns.UP.min(),
      lte: dns.UP.max(),
      values: false
    });

    for await (const {key} of iter) {
      yield dns.UP.decode(key)[0];
    }
  }

  /**
   * Index hourly status.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   * @returns {Promise}
   */

  async indexHourly(batch, entry) {
    const min60 = common.floorTime(entry.time, common.HOUR);
    const bucket60key = dns.STATUS_HOUR_BY_HOST.encode(entry.key, min60);
    const rawStatus60 = await this.bucket.get(bucket60key);
    let oldStatus60 = null;

    if (!rawStatus60) {
      oldStatus60 = new DNSBucketStatus(common.HOUR);
    } else {
      oldStatus60 = DNSBucketStatus.decode(rawStatus60, common.HOUR);
    }

    const newStatus = oldStatus60.clone();
    newStatus.total += 1;
    newStatus.up += entry.isSuccessful() ? 1 : 0;

    const encoded60 = newStatus.encode();
    batch.put(bucket60key, encoded60);

    // Handle UP Count counting.
    // TODO: Maybe clean up old records (e.g. 1 month old).
    const oldTotal = await this.getHourlyUpCount(min60);
    const oldCount = oldStatus60.percentage >= this.onlinePercentile ? 1 : 0;
    const newCount = newStatus.percentage >= this.onlinePercentile ? 1 : 0;
    const newTotal = new TotalOnlineRecord(oldTotal - oldCount + newCount);

    batch.put(dns.UP_COUNT_HOUR.encode(min60), newTotal.encode());
  }

  /**
   * Get hourly statuses by time.
   * @param {String} hostname
   * @param {Number} since
   * @yields {Promise<[Number, DNSBucketStatus]>}
   * @returns {AsyncGenerator<[Number, DNSBucketStatus]>}
   */

  async *getHourlyStatusesByTime(hostname, since) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_HOUR_BY_HOST.encode(hostkey, since),
      lte: dns.STATUS_HOUR_BY_HOST.max(hostkey),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.STATUS_HOUR_BY_HOST.decode(key)[1];
      const json = DNSBucketStatus.decode(value, common.HOUR);
      yield [time, json];
    }
  }

  /**
   * Clean up last hourly statuses by time.
   * @param {String} hostname
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupHourlyStatusesByTime(hostname, before) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_HOUR_BY_HOST.min(hostkey),
      lt: dns.STATUS_HOUR_BY_HOST.encode(hostkey, before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Get hourly UP count.
   * @param {Number} time
   * @returns {Promise<Number>}
   */

  async getHourlyUpCount(time) {
    assert(time % common.HOUR === 0);
    const data = await this.bucket.get(dns.UP_COUNT_HOUR.encode(time));

    if (!data)
      return 0;

    return TotalOnlineRecord.decode(data).total;
  }

  /**
   * Get hourly UP counts by time.
   * @param {Number} since
   * @yields {Promise<[Number, Number]>}
   * @returns {AsyncGenerator<[Number, Number]>}
   */

  async *getHourlyUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_HOUR.encode(since),
      lte: dns.UP_COUNT_HOUR.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.UP_COUNT_HOUR.decode(key)[0];
      yield [time, TotalOnlineRecord.decode(value).total];
    }
  }

  /**
   * Clean up hourly up counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupHourlyUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_HOUR.min(),
      lt: dns.UP_COUNT_HOUR.encode(before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);
    await batch.write();
  }

  /**
   * Index daily status.
   * @param {Batch} batch
   * @param {DNSEntry} entry
   * @returns {Promise}
   */

  async indexDaily(batch, entry) {
    const bucketDay = common.floorTime(entry.time, common.DAY);
    const bucketDayKey = dns.STATUS_DAY_BY_HOST.encode(entry.key, bucketDay);
    const rawStatusDay = await this.bucket.get(bucketDayKey);
    let oldStatusDay = null;

    if (!rawStatusDay) {
      oldStatusDay = new DNSBucketStatus(common.DAY);
    } else {
      oldStatusDay = DNSBucketStatus.decode(rawStatusDay, common.DAY);
    }

    const newStatusDay = oldStatusDay.clone();
    newStatusDay.total += 1;
    newStatusDay.up += entry.isSuccessful() ? 1 : 0;

    const encodedDay = newStatusDay.encode();
    batch.put(bucketDayKey, encodedDay);

    // handle UP Count counting.
    const oldTotal = await this.getDailyUpCount(bucketDay);
    const oldCount = oldStatusDay.percentage >= this.onlinePercentile ? 1 : 0;
    const newCount = newStatusDay.percentage >= this.onlinePercentile ? 1 : 0;
    const newTotal = new TotalOnlineRecord(oldTotal - oldCount + newCount);

    batch.put(dns.UP_COUNT_DAY.encode(bucketDay), newTotal.encode());
  }

  /**
   * Get daily statuses by time.
   * @param {String} hostname
   * @param {Number} since
   * @yields {Promise<[Number, DNSBucketStatus]>}
   * @returns {AsyncGenerator<[Number, DNSBucketStatus]>}
   */

  async *getDailyStatusesByTime(hostname, since) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_DAY_BY_HOST.encode(hostkey, since),
      lte: dns.STATUS_DAY_BY_HOST.max(hostkey),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.STATUS_DAY_BY_HOST.decode(key)[1];
      const json = DNSBucketStatus.decode(value, common.DAY);
      yield [time, json];
    }
  }

  /**
   * Clean up last daily statuses by time.
   * @param {String} hostname
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupDailyStatusesByTime(hostname, before) {
    const hostkey = Buffer.from(hostname, 'utf8');
    const iter = this.bucket.iterator({
      gte: dns.STATUS_DAY_BY_HOST.min(hostkey),
      lt: dns.STATUS_DAY_BY_HOST.encode(hostkey, before)
    });

    const batch = this.bucket.batch();
    for await (const {key} of iter)
      batch.del(key);

    await batch.write();
  }

  /**
   * Get daily UP count.
   * @param {Number} time
   * @returns {Promise<Number>}
   */

  async getDailyUpCount(time) {
    assert(time % common.DAY === 0);
    const data = await this.bucket.get(dns.UP_COUNT_DAY.encode(time));

    if (!data)
      return 0;

    return TotalOnlineRecord.decode(data).total;
  }

  /**
   * Get daily UP counts by time.
   * @param {Number} since
   * @yields {Promise<[Number, Number]>}
   * @returns {AsyncGenerator<[Number, Number]>}
   */

  async *getDailyUpCountsByTime(since) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_DAY.encode(since),
      lte: dns.UP_COUNT_DAY.max(),
      values: true
    });

    for await (const {key, value} of iter) {
      const time = dns.UP_COUNT_DAY.decode(key)[0];
      yield [time, TotalOnlineRecord.decode(value).total];
    }
  }

  /**
   * Clean up daily up counts by time.
   * @param {Number} before
   * @returns {Promise}
   */

  async cleanupDailyUpCounts(before) {
    const iter = this.bucket.iterator({
      gte: dns.UP_COUNT_DAY.min(),
      lt: dns.UP_COUNT_DAY.encode(before)
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

    await cleanUp(dns.STATUS_10_BY_TIME);
    await cleanUp(dns.STATUS_HOUR_BY_TIME);
    await cleanUp(dns.STATUS_DAY_BY_TIME);
  }
}

/*
 * Expose
 */

module.exports = DNSIndexer;
