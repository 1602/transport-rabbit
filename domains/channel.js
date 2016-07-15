'use strict';

module.exports = channel;

const DEFAULT_PREFETCH = 1;

// const createQueueWrapper = require('./queue');
const EventEmitter = require('events');
const assert = require('assert');

function channel(channelName) {
    const debug = require('debug')('rabbit:channel:' + channelName);

    const events = new EventEmitter();
    let amqpChannel = null;

    let prefetchCount = DEFAULT_PREFETCH;
    let prefetchIsGlobal = false;

    const setupHooks = [];
    const bindingHooks = [];

    const channelWrapper = Object.assign(standardChannelInterface(), {
        events,

        bindQueue: (queueName, exchangeName, route, options) => {
            debug(
                'bind "%s" to "%s" exchange using "%s" route',
                queueName,
                exchangeName,
                route);

            return amqpChannel.bindQueue(queueName, exchangeName, route, options);
        },

        addSetup: fn => setupHooks.push(fn),
        addBinding: fn => bindingHooks.push(fn),
        getSettings: () => ({ prefetchCount, prefetchIsGlobal }),
        bind,
        assertOpenChannel,

        get: get
    });

    return channelWrapper;

    function standardChannelInterface() {
        const slice = Array.prototype.slice;
        return [
            'assertExchange',
            'assertQueue',
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
            'recover'
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

    function assertOpenChannel() {
        assert(amqpChannel, 'Client is not connected to channel');
    }

    function get() {
        assertOpenChannel();
        return amqpChannel;
    }

    /**
     * Internal transport to queue bindings.
     *
     * It start listening error and close everts,
     * asserts queues and set up channel (prefetch, etc.).
     *
     * @param channel {AMQPChannel(amqplib)} - amqp channel.
     * @param settings {Object} - { prefetch: Number }.
     */
    function bind(channel, settings) {

        amqpChannel = channel;

        let channelErrored = false;

        channel.on('error', err => {
            debug('Channel error', err.stack);
            channelErrored = true;
            events.emit('error', err);
        });

        channel.on('close', () => {
            debug('Channel %s closed.', channelName);
            events.emit('close', channelErrored);
            amqpChannel = null;
        });

        // channel.on('return', () => {
        // });

        if (settings.prefetch) {
            prefetchCount = settings.prefetch;
        }

        if (settings.channelConfig && settings.channelConfig[channelName]) {
            const prefetchConfig = settings.channelConfig[channelName].prefetch;
            if (prefetchConfig.global) {
                prefetchIsGlobal = true;
            }

            if ('undefined' !== typeof prefetchConfig.count) {
                prefetchCount = prefetchConfig.count;
            }
        }

        debug('setting prefetch to %d, global=%s', prefetchCount, prefetchIsGlobal);

        return channel.prefetch(prefetchCount, prefetchIsGlobal)
            .then(() => Promise.all(setupHooks.map(fn => fn())))
            .then(() => Promise.all(bindingHooks.map(fn => fn())))
            .then(() => {
                debug('channel %s ready', channelName);
            });
    }

}

