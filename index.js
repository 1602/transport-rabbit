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

    const transport = {
        server,
        client,
        send,
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

    function send(name, params) {
        const client = descriptors.find(d => d.type === 'client' &&
            d.spec.name === name);

        if (!latestChannel) {
            throw new Error('Client is not connected to channel');
        }

        console.info('Sending task "%s" to queue "%s"', name, client.spec.produce.queue.name);

        latestChannel.sendToQueue(
            client.spec.produce.queue.name,
            new Buffer(JSON.stringify({
                params
            }))
        );
    }

    function setup() {
        if (state === 'disconnected') {
            state = 'connecting';
            latestConnection = null;
            return setupChannel(openChannel(setupConnection(connect())));
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
                        .then(() => initClients(channel))
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

    function initClients() {
        const clientDescriptors = descriptors
            .filter(d => d.type === 'client')
            .map(d => d.spec);

    }

    function listenServers(channel) {
        const serverDescriptors = descriptors
            .filter(d => d.type === 'server')
            .map(d => d.spec);

        return Promise.all(serverDescriptors
            .map(d => {
                console.log('Will consume', d.consume.queue.name);
                return channel.consume(d.consume.queue.name, msg => {
                    if (msg !== null) {
                        execJob(d.job, channel, msg, d.produce && d.produce.queue.name);
                    }
                })
                    .then(() => console.log('Ready to consume queue',
                                            d.consume.queue.name));
            })
        );
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

        return transport;
    }

    function client(spec) {
        assert(spec.produce, 'Client must have queue to produce to specified');
        addQueue(spec.produce);

        descriptors.push({
            type: 'client',
            spec
        });

        return transport;
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
        console.log('asserting %d queues', queues.length);
        return queues.reduce(
            (flow, q) => flow.then(() => channel.assertQueue(q.name, q.options)),
            Promise.resolve()
        ).then(() => console.log('%d queues asserted', queues.length));
    }

    function execJob(job, channel, msg, respondTo) {
        const data = JSON.parse(msg.content.toString());
        job(data.params)
            .then(result => {
                channel.sendToQueue(
                    respondTo,
                    new Buffer(JSON.stringify({
                        status: 'success',
                        result
                    }))
                );
                channel.ack(msg);
            })
            .catch(error => {
                channel.sendToQueue(
                    respondTo,
                    new Buffer(JSON.stringify({
                        status: 'error',
                        // TODO add external presenter for error
                        error: {
                            message: String(error.message),
                            stack: String(error.stack),
                            details: error.details
                        }
                    }))
                );
                channel.nack(msg);
            });
    }
             
}

