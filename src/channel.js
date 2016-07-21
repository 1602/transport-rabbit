'use strict';

module.exports = channel;

const DEFAULT_SETTINGS = {
    prefetchCount: 1,
    prefetchGlobal: false
};

const assert = require('assert');

module.exports = function createChannel(transport, channelName) {
    const debug = require('debug')('rabbit:channel:' + channelName);

    const settings = _effectiveSettings();

    let amqpChannel = null;

    return Object.assign(standardChannelInterface(), {
        settings,
        assertOpen,
        get: get
    });

    function standardChannelInterface() {
        const slice = Array.prototype.slice;
        return [
            'assertExchange',
            'assertQueue',
            'bindQueue',
            'purgeQueue',
            'checkQueue',
            'deleteQueue',
            'publish',
            'sendToQueue',
            'consume',
            'cancel',
            'get',
            'ack',
            'ackAll',
            'nack',
            'nackAll',
            'reject',
            'prefetch',
            'recover',
            'close'
        ].reduce((wrap, name) => {
            wrap[name] = function() {
                debug(name + ' ' + arguments[0]);
                return get()[name].apply(
                    amqpChannel,
                    slice.call(arguments));
            };
            return wrap;
        }, {});
    }

    function assertOpen() {
        assert(amqpChannel, 'Client is not connected to channel');
    }

    function get() {
        assertOpen();
        return amqpChannel;
    }

    function _init() {
        debug('init');
        return Promise.resolve()
            .then(() => transport.connection.createChannel())
            .then(channel => amqpChannel = channel)
            .then(() => {
                const {
                    prefetchCount,
                    prefetchGlobal
                } = settings;
                debug('setting prefetch=%d, global=%s', prefetchCount, prefetchGlobal);
                return amqpChannel.prefetch(prefetchCount, prefetchGlobal);
            });
    }

    function _effectiveSettings() {
        const chanSettings = transport.settings.channelSettings || {};
        return Object.assign({},
            DEFAULT_SETTINGS,
            chanSettings,
            chanSettings[channelName]);
    }

};

