/*!
 * common.js - Some common constants and functions.
 * Copyright (c) 2024, Nodari Chkuaselidze (MIT License)
 */

'use strict';

exports.SECOND = 1000;
exports.MINUTE = 60 * exports.SECOND;
exports.HOUR = 60 * exports.MINUTE;
exports.DAY = 24 * exports.HOUR;
exports.WEEK = 7 * exports.DAY;
exports.MONTH = 30 * exports.DAY;
exports.YEAR = 365 * exports.DAY;

/**
 * @param {Number} time
 * @param {Number} interval
 * @returns {Number}
 */

exports.floorTime = (time, interval) => {
  return time - (time % interval);
};
