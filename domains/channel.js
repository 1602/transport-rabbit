'use strict';

module.exports = channel;

const DEFAULT_PREFETCH = 1;

const createQueueWrapper = require('./queue');
const EventEmitter = require('events');

const debug = require('debug')('rabbit:channel');

function channel() {
    const events = new EventEmitter();
    let currentChannel = null;

    const channelWrapper = Object.assign(standardChannelInterface(), {
        events,
        bind,
        get: get
    });

    const queueWrapper = createQueueWrapper(channelWrapper);

    return channelWrapper;

    function standardChannelInterface() {
        const slice = Array.prototype.slice;
        return [
            'publish', 'sendToQueue', 'consume',
            'cancel', 'get', 'ack', 'ackAll',
            'nack', 'nackAll', 'reject', 'prefetch', 'recover'
        ].reduce((wrap, name) => {
            wrap[name] = function() {
                return get()[name].apply(
                    currentChannel,
                    slice.call(arguments));
            };
            return wrap;
        }, {});
    }

    function get() {
        if (!currentChannel) {
            throw new Error('Client is not connected to channel');
        }
        return currentChannel;
    }

    /**
     * Internal transport to queue bindings
     * @param channel {AMQPChannel(amqplib)} - amqp channel
     * @param queues {Array} - queue descriptors
     * @param settings {Object} - { prefetch: Number }
     */
    function bind(channel, queues, settings) {

        currentChannel = channel;

        let channelErrored = false;

        channel.on('error', err => {
            debug('Channel error', err.stack);
            channelErrored = true;
            events.emit('error', err);
        });

        channel.on('close', () => {
            debug('Channel closed.');
            events.emit('close', channelErrored);
            currentChannel = null;
        });

        const prefetchCount = settings.prefetch || DEFAULT_PREFETCH;

        channel.prefetch(prefetchCount)
            .then(() => assertQueues(queues))
            .then(() => {
                debug('Channel ready');
                events.emit('ready');
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

        const ch = currentChannel;

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

                return queueWrapper.assert(
                    queueName,
                    q.options
                )
                    .then(asserted => {
                        q.queueNames[route] = asserted.queue;
                        return queueWrapper.bind(
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

