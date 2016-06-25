'use strict';

const assert = require('assert');
const debug = require('debug')('transport');

const createConnection = require('./connection');
const createChannel = require('./channel');

module.exports = initTransport;

function initTransport(settings) {

    let latestChannel = null;

    const queues = [];
    const descriptors = [];
    const rpcCallbackQueues = Object.create(null);
    const awaitingResponseHandlers = Object.create(null);
    const awaitingExpiration = [];

    const expirationInterval = setInterval(() => {
        debug('Expire rpc');
        const now = Date.now();
        let expireMe;
        while (awaitingExpiration[0] && awaitingExpiration[0].expireAt <= now) {
            expireMe = awaitingExpiration.shift();
            expireMe.deferred.reject(new Error('Awaiting response handler expired by timeout'));
            delete awaitingResponseHandlers[expireMe.correlationId];
        }
    }, 1000);

    const EventEmitter = require('events');
    const events = new EventEmitter();

    const transport = {
        server,
        client,
        rpc,
        events,
        getReady,
        close() {
            clearInterval(expirationInterval);
            connection.close();
        },
        isConnected: () => connection.isConnected()
    };

    function getReady() {
        return new Promise(resolve => {
            events.on('ready', resolve);
        });
    }

    transport.queue = {
        assert: (queueName, options) =>
            latestChannel.assertQueue(queueName, options),

        delete: (queueName, options) =>
            latestChannel.deleteQueue(queueName, options),

        check: queueName =>
            latestChannel.checkQueue(queueName),

        purge: queueName =>
            latestChannel.purgeQueue(queueName),

        messageCount:
            queueName => transport.queue.check(queueName)
                .then(check => check.messageCount),

        consume: (queueName, fn) =>
            latestChannel.consume(queueName, msg => {
                if (msg !== null) {
                    fn(msg);
                }
            })
    };

    if (settings.quitGracefullyOnTerm) {
        process.once('SIGTERM', transport.close);
        process.once('SIGINT', transport.close);
    }

    const connection = createConnection(settings);
    const channel = createChannel();

    connection.events.on('connected', () =>
        connection.createChannel()
            .catch(err => {
                // might happen if more than MAX_CHANNELS channels created
                // 65536 by default in rabbit 3
                console.error('Error while creating channel. Closing connection.', err);
                connection.close();
            })
            .then(ch => channel.bind(ch, settings))
            .catch(err => transport.events.emit('error', err))
    );

    channel.events.on('close', channelErrored => {
        if (channelErrored && !connection.isDisconnected()) {
            connection.forceClose();
        }
    });

    channel.events.on('ready', () => {
        setupChannel(channel.get())
            .then(() => transport.events.emit('ready'));
    });

    return transport;

    function setupChannel(channel) {

        latestChannel = channel;

        return Promise.resolve()
            .then(() => assertQueues())
            .then(() => Promise.all([listenServers(), initRPC()]))
            .then(() => latestChannel);
    }

    function initRPC() {
        const rpcDescriptors = descriptors
            .filter(d => d.type === 'rpc')
            .map(d => d.spec);

        return Promise.all(rpcDescriptors
            .map(d => {
                const exchange = d.produce.queue.exchange;
                const route = d.produce.queue.routes[0];
                const requestQueue = [ exchange, route ].join('.');

                return Promise.all([
                    transport.queue.assert('', { exclusive: true }),
                    transport.queue.assert('', { exclusive: true })
                ])
                    .then(queues => {
                        const reply = queues[0];
                        const error = queues[1];

                        rpcCallbackQueues[requestQueue] = { reply, error };

                        return Promise.all([
                            consume(reply.queue, 'resolve'),
                            consume(error.queue, 'reject')
                        ]);

                    });
            }));

        function consume(queueName, action) {
            return transport.queue.consume(queueName, msg => {
                debug('Received', (msg.properties.type || 'message'), 'to', queueName);
                latestChannel.ack(msg);
                const data = JSON.parse(msg.content.toString());
                const corrId = msg.properties.correlationId;
                awaitingResponseHandlers[corrId].deferred[action](data.payload);
                delete awaitingResponseHandlers[corrId];
                awaitingExpiration.splice(awaitingExpiration.indexOf(awaitingResponseHandlers[corrId]), 1);
            });
        }
    }

    function listenServers() {
        const channel = latestChannel;
        const serverDescriptors = descriptors
            .filter(d => d.type === 'server')
            .map(d => d.spec);

        return Promise.all(serverDescriptors
            .map(d => {
                debug('Will consume', d.consume.queue.exchange, d.consume.queue.routes);
                return Promise.all(d.consume.queue.routes.map(route => {
                    const queueName = d.consume.queue.queueNames[route];

                    return transport.queue.consume(queueName, msg => {
                        debug('Received', (msg.properties.type || 'message'), 'to', queueName);
                        execJob(d.handler[route], channel, msg, d.produce && d.produce.queue.exchange, d.getContextById);
                    })
                        .then(() => debug('Ready to consume queue %s (%s)',
                                                queueName, route));
                }));
            }));
    }

    function addQueue(queueDescriptor) {
        assert(isValidQueueDescriptor(queueDescriptor), 'Invalid queue descriptor (consume)');
        queues.push(queueDescriptor.queue);
    }

    function isValidQueueDescriptor(queueDescriptor) {
        return queueDescriptor instanceof Object && queueDescriptor.queue instanceof Object;
    }

    function server(spec) {
        assert(spec.consume, 'Server must have queue to consume from specified');
        addQueue(spec.consume);

        descriptors.push({
            type: 'server',
            spec
        });

        if (spec.produce) {
            addQueue(spec.produce);
        }
    }

    function client(spec) {
        assert(spec.produce, 'Client must have queue to produce to specified');
        addQueue(spec.produce);

        descriptors.push({
            type: 'client',
            spec
        });

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes && spec.produce.queue.routes[0];
        const requestQueue = [ exchange, route ].join('.');

        return function send(payload, toRoute, opts) {
            if (!latestChannel) {
                throw new Error('Client is not connected to channel');
            }

            Promise.resolve(getCorrelationId(opts && opts.context))
                .then(correlationId => {
                    debug('Sending msg to queue "%s"', requestQueue, toRoute || route, 'corrId =', correlationId);

                    return latestChannel.publish(
                        exchange,
                        toRoute || route,
                        new Buffer(JSON.stringify({ payload })),
                        { correlationId }
                    );
                });
        };

        function getCorrelationId(context) {
            if (context && spec.getContextId) {
                return spec.getContextId(context);
            }

            return generateId();
        }
    }

    function rpc(spec) {
        assert(spec.produce, 'Client must have queue to produce msg to specified');
        addQueue(spec.produce);

        descriptors.push({
            type: 'rpc',
            spec
        });

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes[0];
        const requestQueue = [ exchange, route ].join('.');

        return function send(payload) {
            if (!latestChannel) {
                throw new Error('Client is not connected to channel');
            }

            const correlationId = generateId();
            const deferred = Promise.defer();

            const responseHandler = {
                correlationId,
                deferred,
                startedAt: Date.now()
            };

            if (settings.rpcTimeout) {
                responseHandler.expireAt = responseHandler.startedAt + settings.rpcTimeout;
                awaitingExpiration.push(responseHandler);
            }

            awaitingResponseHandlers[correlationId] = responseHandler;


            debug('Sending msg to queue "%s"', requestQueue);

            latestChannel.publish(
                exchange,
                route,
                new Buffer(JSON.stringify({ payload })),
                {
                    correlationId,
                    replyTo: [
                        rpcCallbackQueues[requestQueue].reply.queue,
                        rpcCallbackQueues[requestQueue].error.queue
                    ].join('|')
                }
            );

            return deferred.promise;
        };

    }

    function assertQueues() {
        if (!queues.length) {
            return;
        }

        const channel = latestChannel;

        debug('asserting %d exchanges', queues.length);
        return queues.reduce(
            (flow, q) => flow.then(() => channel.assertExchange(
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

                return channel.assertQueue(
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

                        return channel.bindQueue(
                            asserted.queue,
                            q.exchange,
                            route,
                            q.options
                        );
                    });
            }));
        }
    }

    function execJob(handler, channel, msg, respondTo, getContextById) {
        const data = JSON.parse(msg.content.toString());

        Promise.resolve(getContext())
            .then(context => handler(data.payload, context))
            .then(payload => reply('result', payload))
            // TODO add external presenter for error
            .catch(error => reply('error', {
                message: String(error.message),
                stack: String(error.stack),
                details: error.details
            }));

        function reply(type, payload) {

            channel.ack(msg);

            const replyTo = parseReplyTo(type, msg.properties.replyTo);
            const correlationId = msg.properties.correlationId;

            if (replyTo) {
                debug('Reply with %s to queue %s', type, replyTo);

                channel.sendToQueue(
                    replyTo,
                    new Buffer(JSON.stringify({ payload })),
                    { correlationId, type }
                );
            } else if (respondTo) {
                debug('Reply with %s to exchange %s', type, respondTo);

                channel.publish(
                    respondTo,
                    type,
                    new Buffer(JSON.stringify({ payload })),
                    { correlationId, type }
                );
            }
        }

        function parseReplyTo(type, replyTo) {
            const method = type === 'result' ? 'shift' : 'pop';
            return replyTo && replyTo.split('|')[method]();
        }

        function getContext() {
            if ('function' !== typeof getContextById) {
                return null;
            }

            if (!msg.properties.correlationId) {
                return null;
            }

            return getContextById(msg.properties.correlationId);
        }
    }

    function generateId() {
        return Math.random().toString(36).substr(2) +
            Math.random().toString(36).substr(1) +
            Math.random().toString(36).substr(1);
    }
             
}

