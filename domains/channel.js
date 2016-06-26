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

    function bind(channel, queues, settings) {

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
            .then(() => assertQueues(queues))
            .then(() => {
                debug('Channel ready');
                events.emit('ready');
            });
    }

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

                return ch.assertQueue(
                    queueName,
                    q.options
                )
                    .then(asserted => {
                        q.queueNames[route] = asserted.queue;
                        debug(
                            'bind "%s" to "%s" exchange using "%s" route',
                            asserted.queue,
                            q.exchange,
                            route);

                        return ch.bindQueue(
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

