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

        bind,
        assertOpenChannel,

        get: get
    });

    return channelWrapper;

    function standardChannelInterface() {
        const slice = Array.prototype.slice;
        return [
            'assertQueue',
            'purgeQueue',
            'checkQueue',
            'deleteQueue',
            'publish', 'sendToQueue', 'consume',
            'cancel', 'get', 'ack', 'ackAll',
            'nack', 'nackAll', 'reject', 'prefetch', 'recover'
        ].reduce((wrap, name) => {
            wrap[name] = function() {
                if (name === 'assertQueue') {
                    debug('assertQueue', arguments[0], arguments[1]);
                }
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
     * @param queues {Array} - queue descriptors.
     * @param settings {Object} - { prefetch: Number }.
     */
    function bind(channel, queues, settings) {

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

        const prefetchCount = settings.prefetch || DEFAULT_PREFETCH;

        return channel.prefetch(prefetchCount, true)
            .then(() => assertQueues(queues))
            .then(() => {
                debug('channel %s ready', channelName);
            });
    }

    /**
     * Assert known queues
     * @param queues {Array} - array of queue specs:
     *  - {exchange, exchangeType, autogenerateQueues, routes, options}
     */
    function assertQueues(queues) {
        if (!queues.length) {
            return;
        }

        const ch = amqpChannel;

        debug('asserting %d exchanges', queues.length);
        return queues.reduce(
            (flow, q) => flow.then(() => ch.assertExchange(
                    q.exchange,
                    q.exchangeType || 'direct'
                )
                    .then(() => {
                        if (q.routes) {
                            return assertRoutes(q.routes, q);
                        }
                    })
            ),
            Promise.resolve()
        ).then(() => debug('%d exchanges asserted', queues.length));

        function assertRoutes(routes, q) {

            return Promise.all(routes.map(route => {
                const queueName = q.autogenerateQueues
                    ? ''
                    : [ q.exchange, route ].join('.');

                q.queueNames = {};

                return channelWrapper.assertQueue(
                    queueName,
                    q.options
                )
                    .then(asserted => {
                        q.queueNames[route] = asserted.queue;
                        return channelWrapper.bindQueue(
                            asserted.queue,
                            q.exchange,
                            route,
                            q.options
                        );
                    });
            }));
        }
    }
}

