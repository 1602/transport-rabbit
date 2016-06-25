'use strict';

module.exports = channel;

const DEFAULT_PREFETCH = 1;

const EventEmitter = require('events');

const debug = require('debug')('rabbit:channel');

function channel() {
    const events = new EventEmitter();
    let currentChannel;

    return {
        events,
        bind,
        get: () => {
            if (!currentChannel) {
                throw new Error('Client is not connected to channel');
            }
            return currentChannel;
        }
    };

    function bind(channel, settings) {

        currentChannel = channel;

        let channelErrored = false;

        channel.on('error', err => {
            debug('Channel error', err);
            channelErrored = true;
            events.emit('error', err);
        });

        channel.on('close', () => {
            debug('Channel closed.');
            events.emit('close', channelErrored);
        });

        const prefetchCount = settings.prefetch || DEFAULT_PREFETCH;

        channel.prefetch(prefetchCount)
            .then(() => {
                debug('Channel ready');
                events.emit('ready');
            });
    }

}

