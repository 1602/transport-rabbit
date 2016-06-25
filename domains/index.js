'use strict';

const assert = require('assert');
const debug = require('debug')('transport');

const createConnection = require('./connection');
const createChannel = require('./channel');
const createRpcFabric = require('./rpc');
const createClientFabric = require('./client');
const createServerFabric = require('./server');
const queue = require('./queue');

module.exports = initTransport;

function initTransport(settings) {

    const queues = [];

    const EventEmitter = require('events');
    const events = new EventEmitter();

    const transport = {
        events,
        getReady,
        close() {
            events.emit('close');
            return connection.close();
        },
        addQueue: spec => addQueue(spec),
        isConnected: () => connection.isConnected()
    };

    function getReady() {
        return new Promise(resolve => {
            events.on('ready', resolve);
        });
    }

    if (settings.quitGracefullyOnTerm) {
        process.once('SIGTERM', transport.close);
        process.once('SIGINT', transport.close);
    }

    const connection = createConnection(settings);
    const channel = createChannel();

    transport.queue = queue(transport, channel);

    const rpc = createRpcFabric(transport, channel, settings);
    transport.rpc = spec => rpc.declare(spec);

    const client = createClientFabric(transport, channel);
    transport.client = spec => client.declare(spec);

    const server = createServerFabric(transport, channel);
    transport.server = spec => server.declare(spec);

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
        setupChannel().then(() => transport.events.emit('ready'));
    });

    return transport;

    function setupChannel() {
        return Promise.resolve()
            .then(() => assertQueues())
            .then(() => Promise.all([server.init(), rpc.init()]));
    }

    function addQueue(queueDescriptor) {
        assert(isValidQueueDescriptor(queueDescriptor), 'Invalid queue descriptor (consume)');
        queues.push(queueDescriptor.queue);
    }

    function isValidQueueDescriptor(queueDescriptor) {
        return queueDescriptor instanceof Object && queueDescriptor.queue instanceof Object;
    }

    function assertQueues() {
        if (!queues.length) {
            return;
        }

        const ch = channel.get();

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

