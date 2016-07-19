'use strict';

const debug = require('debug')('rabbit:job');

/**
 * Constructs a consumer job, which is passed as a second
 * argument to `consume` and derived methods.
 *
 * Job's responsibilities are:
 *
 *   * keep reference to original `msg`
 *   * provide ACK/NACK methods
 *   * maintain `handled` state (e.g. if job is NACKed, handling should not happen)
 */
module.exports = function createJob(msg, channel, consumerOptions) {
    const {
        noAck
    } = (consumerOptions || {});

    let ackStatus = noAck === true ? 'ack' : null;

    return {
        msg,
        ack,
        nack,
        get ackStatus() {
            return ackStatus
        }
    };

    function ack() {
        if (ackStatus) {
            return;
        }
        debug('ack');
        ackStatus = 'ack';
        channel.ack(msg);
    }

    function nack() {
        if (ackStatus) {
            return;
        }
        debug('nack');
        ackStatus = 'nack';
        channel.nack(msg);
    }
    
};
