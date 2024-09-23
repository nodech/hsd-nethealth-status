/*!
 * layout.js - DB layout for node statuses.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 * https://github.com/nodech/hsd-nethealth-status
 */

'use strict';

const bdb = require('bdb');

/*
 * Aggregated status per day:
 *   - last feature set
 *   - total pings
 *   - total successes (calc percentage later)
 * Used for per month and per year stats.
 */

exports.statusDB = {
  // General 0x00 - 0x1f (30)
  VERSION: bdb.key(0x00),

  // Indexers 0x20 - 0x3f (31)
  DNS: bdb.key(0x20, ['uint32']),
  NODE: bdb.key(0x21, ['uint32'])
};

const DNS_HOSTNAME = 'buffer';
const DNS_TIMESTAMP = 'uint64';

// IP + PORT
const NODE_HOSTPORT = 'buffer';
const NODE_TIMESTAMP = 'uint64';

exports.dns = {
  prefix: bdb.key(0x20),

  // pointer to the log index
  LAST_TIMESTAMP: bdb.key(0x00),

  // Now indexes 0x10 - 0x0f (15)
  // By hostname indexes
  // hostname -> status (on/off) & timestamp
  LAST_STATUS: bdb.key(0x10, [DNS_HOSTNAME]),

  // hostname -> timestamp
  LAST_UP: bdb.key(0x11, [DNS_HOSTNAME]),

  // Up only. hostname -> dummy
  UP: bdb.key(0x12, [DNS_HOSTNAME]),
  // Latest online count.
  UP_COUNT: bdb.key(0x13),

  UP_COUNT_10: bdb.key(0x14, [DNS_TIMESTAMP]),
  UP_COUNT_HOUR: bdb.key(0x15, [DNS_TIMESTAMP]),
  UP_COUNT_DAY: bdb.key(0x16, [DNS_TIMESTAMP]),

  // 10 min - hostname, timestamp -> status (on/off)
  STATUS_10_BY_HOST: bdb.key(0x20, [DNS_HOSTNAME, DNS_TIMESTAMP]),
  // Deprecated
  STATUS_10_BY_TIME: bdb.key(0x21, [DNS_TIMESTAMP, DNS_HOSTNAME]),

  // 1 hour buckets - hostname, timestamp -> {upCount, totalPings}
  STATUS_HOUR_BY_HOST: bdb.key(0x22, [DNS_HOSTNAME, DNS_TIMESTAMP]),
  // Deprecated
  STATUS_HOUR_BY_TIME: bdb.key(0x23, [DNS_TIMESTAMP, DNS_HOSTNAME]),

  // 1 day buckets - hostname, timestamp -> {upCount, totalPings}
  STATUS_DAY_BY_HOST: bdb.key(0x24, [DNS_HOSTNAME, DNS_TIMESTAMP]),
  // Deprecated
  STATUS_DAY_BY_TIME: bdb.key(0x25, [DNS_TIMESTAMP, DNS_HOSTNAME])
};

exports.nodes = {
  prefix: bdb.key(0x21),

  // pointer to the log index
  LAST_TIMESTAMP: bdb.key(0x00),

  // By hostname indexes
  // hostname -> status
  LAST_STATUS: bdb.key(0x10, [NODE_HOSTPORT]),

  // Last successful node entry.
  // hostname -> NodeEntry
  LAST_UP: bdb.key(0x11, [NODE_HOSTPORT]),

  // Port mappings
  // host -> port
  PORT_MAPPINGS: bdb.key(0x12, [NODE_HOSTPORT, 'uint16']),

  // Up only. hostname -> dummy
  UP: bdb.key(0x13, [NODE_HOSTPORT]),

  // Latest online count.
  UP_COUNTS: bdb.key(0x14),
  UP_COUNTS_10: bdb.key(0x15, [NODE_TIMESTAMP]),
  UP_COUNTS_HOUR: bdb.key(0x16, [NODE_TIMESTAMP]),
  UP_COUNTS_DAY: bdb.key(0x17, [NODE_TIMESTAMP]),

  // By hostname and timestamp stuff.
  STATUS_10_BY_HOST: bdb.key(0x20, [NODE_HOSTPORT, NODE_TIMESTAMP]),
  STATUS_10_BY_TIME: bdb.key(0x21, [NODE_TIMESTAMP, NODE_HOSTPORT]),

  STATUS_HOUR_BY_HOST: bdb.key(0x22, [NODE_HOSTPORT, NODE_TIMESTAMP]),
  STATUS_HOUR_BY_TIME: bdb.key(0x23, [NODE_TIMESTAMP, NODE_HOSTPORT]),

  STATUS_DAY_BY_HOST: bdb.key(0x24, [NODE_HOSTPORT, NODE_TIMESTAMP]),
  STATUS_DAY_BY_TIME: bdb.key(0x25, [NODE_TIMESTAMP, NODE_HOSTPORT])
};
