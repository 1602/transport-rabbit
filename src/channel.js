'use strict';

const DEFAULT_SETTINGS = {
    prefetchCount: 1,
    prefetchGlobal: false
};

const assert = require('assert');

module.exports = function createChannel(transport, channelName) {
    const debug = require('debug')('rabbit:channel:' + channelName);

    const settings = getEffectiveSettings();

    let amqpChannel = null;
    
    transport.addInit(init);

    return Object.assign(standardChannelInterface(), {
        settings
    });

    function standardChannelInterface() {
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
            wrap[name] = function(...args) {
                debug(name, ...args);
                return getWrappedChannel[name](...args);
            };
            return wrap;
        }, {});
    }

    function getWrappedChannel() {
        assert(amqpChannel, 'Client is not connected to channel');
        return amqpChannel;
    }

    function init() {
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

    function getEffectiveSettings() {
        const chanSettings = transport.settings.channelSettings || {};
        return Object.assign({},
            DEFAULT_SETTINGS,
            chanSettings,
            chanSettings[channelName]);
    }

};

