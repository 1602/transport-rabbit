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

        const initializers = [];
        // for sequential run
        let currentInitializer = Promise.resolve();
        let initialized = false;
        let amqpChannel = null;

        const disconnect = transport.onConnected(onConnected);

        const channel = Object.assign(standardChannelInterface(), {
            getWrappedChannel,
            settings: effectiveSettings,
            addInit,
            getReady,
            close
        });

        addInit(() => openChannel());

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
            assert(amqpChannel, 'Transport not connected');
            return amqpChannel;
        }

        function onConnected() {
            debug('init');
            return runInitializers()
                .catch(err => onInitError(err));
        }

        function addInit(fn) {
            if (initialized) {
                runInit(fn).catch(err => onInitError(err));
            }
            initializers.push(fn);
            return function removeInit() {
                const i = initializers.indexOf(fn);
                if (i > -1) {
                    initializers.splice(i, 1);
                }
            };
        }

        function runInitializers() {
            initialized = false;
            // imperial loops! please don't try to optimize these
            // b/c their side-effects are crucial to our success
            initializers.forEach(fn => runInit(fn));
            initialized = true;
            return currentInitializer;
        }

        function runInit(fn) {
            currentInitializer = currentInitializer.then(() => fn());
            return currentInitializer;
        }

        function onInitError(err) {
            console.error('Error during channel initialization', err);
        }

        function getReady() {
            return currentInitializer;
        }

        function openChannel() {
            return Promise.resolve()
                .then(() => transport.getConnection().createChannel())
                .then(channel => amqpChannel = channel)
                .then(() => channel.prefetch(
                    effectiveSettings.prefetchCount,
                    effectiveSettings.prefetchGlobal));
        }

        function close() {
            debug('close');
            if (amqpChannel) {
                amqpChannel.close();
                amqpChannel = null;
                disconnect();
            }
            delete transport.channels[channelName];
        }

    }

};
