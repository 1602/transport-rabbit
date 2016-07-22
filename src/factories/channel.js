'use strict';

const createDebug = require('debug');

const DEFAULT_SETTINGS = {
    prefetchCount: 1,
    prefetchGlobal: false
};

const CHANNEL_METHODS = [
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
];

const VERBOSE_DEBUG = [
    'assertExchange',
    'assertQueue',
    'bindQueue',
    'prefetch'
];

const assert = require('assert');

module.exports = function createChannelFactory(transport) {

    return function assertChannel(channelName, settings) {
        assert(channelName, 'channelName not specified');

        if (!(channelName in transport.channels)) {
            transport.channels[channelName] = createChannel(channelName, settings);
        }
        return transport.channels[channelName];
    };

    function createChannel(channelName, settings) {
        const debug = createDebug('rabbit:channel:' + channelName);

        const effectiveSettings = Object.assign({}, DEFAULT_SETTINGS, settings);

        let amqpChannel = null;

        transport.addInit(init);

        const channel = Object.assign(standardChannelInterface(), {
            getWrappedChannel,
            settings: effectiveSettings
        });

        return channel;

        function standardChannelInterface() {
            return CHANNEL_METHODS.reduce((wrap, name) => {
                wrap[name] = function(...args) {
                    if (VERBOSE_DEBUG.indexOf(name) > -1) {
                        debug(name, ...args);
                    } else {
                        debug(name);
                    }
                    return getWrappedChannel()[name](...args);
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
                .then(() => transport.getConnection().createChannel())
                .then(channel => amqpChannel = channel)
                .then(() => channel.prefetch(
                    effectiveSettings.prefetchCount,
                    effectiveSettings.prefetchGlobal));
        }

    }

};
