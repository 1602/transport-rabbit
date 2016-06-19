'use strict';

const amqplib = require('amqplib');
const assert = require('assert');

const DEFAULT_PREFETCH = 1;

module.exports = initTransport;

function initTransport(settings) {

    let reconnect = settings.reconnect;
    let latestConnection = null;
    let latestChannel = null;
    let state = 'disconnected';

    const amqpServerUrl = settings.url;

    const queues = [];
    const descriptors = [];
    const rpcCallbackQueues = Object.create(null);
    const awaitingResponseHandlers = Object.create(null);

    const transport = {
        server,
        client,
        rpc,
        close() {
            state = 'disconnected';
            reconnect = false;
            latestConnection.close();
        },
        isConnected: () => state === 'connected'
    };

    if (settings.quitGracefullyOnTerm) {
        process.once('SIGTERM', transport.close);
        process.once('SIGINT', transport.close);
    }

    setup();

    return transport;

    function setup() {
        if (state === 'disconnected') {
            state = 'connecting';
            latestConnection = null;
            return setupChannel(openChannel(setupConnection(connect())))
                .catch(err => {
                    console.err('Error during setup', err);
                });
        }

        console.error('Connection state is "%s", will not setup new connection');
    }

    function setupChannel(channel) {
        return Promise.resolve(channel)
            .then(channel => {
                if (channel) {
                    channel.prefetch(settings.prefetch || DEFAULT_PREFETCH);
                    channel.on('error', err => console.error('Channel error', err));
                    channel.on('close', () => {
                        if (reconnect) {
                            console.error('Channel closed.');
                        } else {
                            console.error('Channel closed. Let it be closed.');
                            if (state !== 'disconnected') {
                                latestConnection.close();
                            }
                        }
                    });

                    return assertQueues(channel, queues)
                        .then(() => listenServers(channel))
                        .then(() => initRPC(channel))
                        .then(() => latestChannel = channel);
                }
            });
    }

    function setupConnection(connection) {
        return Promise.resolve(connection)
            .then(connection => {
                connection.on('error', err => {
                    console.log('Connection error', err);
                });
                connection.on('close', () => {
                    state = 'disconnected';
                    if (reconnect) {
                        console.log('Connection closed. Will reconnect in a moment');
                        setTimeout(setup, 1000);
                    } else {
                        console.log('Connection closed. Will not reconnect');
                    }
                });
                latestConnection = connection;
                state = 'connected';
                return connection;
            });
    }

    function initRPC(channel) {
        const rpcDescriptors = descriptors
            .filter(d => d.type === 'rpc')
            .map(d => d.spec);

        return Promise.all(rpcDescriptors
            .map(d => {
                const exchange = d.produce.queue.exchange;
                const route = d.produce.queue.routes[0];
                const requestQueue = [ exchange, route ].join('.');

                return Promise.all([
                    channel.assertQueue('', { exclusive: true }),
                    channel.assertQueue('', { exclusive: true })
                ])
                    .then(queues => {
                        const reply = queues[0];
                        const error = queues[1];
                        rpcCallbackQueues[requestQueue] = { reply, error };

                        return Promise.all([
                            channel.consume(reply.queue, msg => {
                                if (msg === null) {
                                    return;
                                }
                                channel.ack(msg);
                                const data = JSON.parse(msg.content.toString());
                                const corrId = msg.properties.correlationId;
                                awaitingResponseHandlers[corrId].resolve(data.payload);
                                delete awaitingResponseHandlers[corrId];
                            }),
                            channel.consume(error.queue, msg => {
                                if (msg === null) {
                                    return;
                                }
                                channel.ack(msg);
                                const data = JSON.parse(msg.content.toString());
                                const corrId = msg.properties.correlationId;
                                awaitingResponseHandlers[corrId].reject(data.payload);
                                delete awaitingResponseHandlers[corrId];
                            })
                        ]);
                    });
            }));
    }

    function listenServers(channel) {
        const serverDescriptors = descriptors
            .filter(d => d.type === 'server')
            .map(d => d.spec);

        return Promise.all(serverDescriptors
            .map(d => {
                console.log('Will consume', d.consume.queue.exchange, d.consume.queue.routes);
                return Promise.all(d.consume.queue.routes.map(route => {
                    const queueName = [d.consume.queue.exchange, route].join('.');

                    return channel.consume(queueName, msg => {
                        console.log('Received msg to', queueName);
                        if (msg !== null) {
                            execJob(d.handler[route], channel, msg, d.produce && d.produce.queue.exchange);
                        }
                    })
                        .then(() => console.log('Ready to consume queue',
                                                queueName));
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
    }

    function rpc(spec) {
        assert(spec.produce, 'Client must have queue to produce to specified');
        addQueue(spec.produce);

        descriptors.push({
            type: 'rpc',
            spec
        });

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes[0];
        const requestQueue = [ exchange, route ].join('.');
        const correlationId = generateId();
        const deferred = Promise.defer();
        awaitingResponseHandlers[correlationId] = deferred;

        return function send(payload) {

            if (!latestChannel) {
                throw new Error('Client is not connected to channel');
            }

            console.info('Sending msg to queue "%s"', requestQueue);

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
        }

    }

    function connect() {
        return amqplib.connect(amqpServerUrl)
            .catch(err => {
                if (reconnect) {
                    console.error('Error while connecting, will try to reconnect',
                                  err);

                    return new Promise(r => setTimeout(() => r(connect()), 2000))
                }

                throw err;
            });
    }

    function openChannel(connection) {
        return Promise.resolve(connection)
            .then(connection => {
                return connection.createChannel()
                    .catch(err => {
                        console.error('Error while creating channel,',
                                      'closing connection.', err);
                        connection.close();
                    });
            })
            .then(channel => {
                if (channel) {
                    console.log('Connected to queue')
                }
                return channel;
            });
    }

    function assertQueues(channel, queues) {
        console.log('asserting %d exchanges', queues.length);
        return queues.reduce(
            (flow, q) => flow.then(() => {
                return Promise.all(q.routes.map(route => {
                    const queueName = [ q.exchange, route ].join('.');

                    console.log('bind "%s" to "%s" exchange', queueName, q.exchange);
                    return channel.assertExchange(q.exchange, 'direct')
                        .then(() => channel.assertQueue(
                            queueName,
                            q.options
                        ))
                        .then(() => channel.bindQueue(
                            queueName,
                            q.exchange,
                            route,
                            q.options
                        ));
                }));
            }),
            Promise.resolve()
        ).then(() => console.log('%d exchanges asserted', queues.length));
    }

    function execJob(handler, channel, msg, respondTo) {
        const data = JSON.parse(msg.content.toString());
        Promise.resolve(handler(data.payload))
            .then(payload => {

                channel.ack(msg);

                const replyTo = msg.properties.replyTo &&
                    msg.properties.replyTo.split('|').shift();

                if (replyTo) {
                    channel.sendToQueue(
                        replyTo,
                        new Buffer(JSON.stringify({ payload })),
                        { correlationId: msg.properties.correlationId }
                    );
                    return;
                }

                if (!respondTo) {
                    return;
                }

                channel.publish(
                    respondTo,
                    'result',
                    new Buffer(JSON.stringify({
                        payload
                    }))
                );
            })
            .catch(error => {

                channel.ack(msg);

                const replyTo = msg.properties.replyTo &&
                    msg.properties.replyTo.split('|').pop();

                // TODO add external presenter for error
                const payload = {
                    message: String(error.message),
                    stack: String(error.stack),
                    details: error.details
                };

                console.log(error, replyTo);

                if (replyTo) {
                    channel.sendToQueue(
                        replyTo,
                        new Buffer(JSON.stringify({ payload })),
                        { correlationId: msg.properties.correlationId }
                    );
                    return;
                }

                if (!respondTo) {
                    return;
                }

                channel.publish(
                    respondTo,
                    'error',
                    new Buffer(JSON.stringify({ payload }))
                );
            });
    }

    function generateId() {
        return Math.random().toString(36).substr(2) +
            Math.random().toString(36).substr(1) +
            Math.random().toString(36).substr(1);
    }
             
}

