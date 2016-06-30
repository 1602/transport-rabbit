'use strict';

const assert = require('assert');

const createConnection = require('./connection');
const createChannel = require('./channel');
const createRpcFabric = require('./rpc');
const createClientFabric = require('./client');
const createServerFabric = require('./server');
const createPubsubFabric = require('./pubsub');
const createCommandFabric = require('./command');
const queue = require('./queue');
const debug = require('debug')('rabbit:transport');

module.exports = initTransport;

function initTransport(settings) {

    const queues = [];

    const EventEmitter = require('events');
    const events = new EventEmitter();

    const transport = {
        events,
        getReady: () => new Promise(resolve => events.on('ready', resolve)),
        close: () => connection.close(),
        addQueue,
        isConnected: () => connection.isConnected()
    };

    const connection = createConnection(settings);
    const channel = createChannel();

    transport.connection = connection;
    transport.channel = channel;

    transport.queue = queue(channel);

    const rpc = createRpcFabric(transport, channel, settings);
    transport.rpc = spec => rpc.declare(spec);

    const client = createClientFabric(transport, channel);
    transport.client = spec => client.declare(spec);

    const server = createServerFabric(transport, channel);
    transport.server = spec => server.declare(spec);

    const pubsub = createPubsubFabric(transport);
    transport.broadcaster = pubsub.createBroadcaster;
    transport.receiver = pubsub.createReceiver;

    const command = createCommandFabric(transport);
    transport.createCommandSender = command.createCommandSender;
    transport.createCommandServer = command.createCommandServer;
    transport.createCommandResultRecipient = command.createCommandResultRecipient;

    connection.events.on('connected', () =>
        connection.createChannel()
            .catch(err => {
                // might happen if more than MAX_CHANNELS channels created
                // 65536 by default in rabbit version 3
                debug('Error while creating channel. Closing connection.', err);
                connection.close();
            })
            .then(ch => channel.bind(ch, queues, settings))
            .catch(err => transport.events.emit('error', err))
    );

    connection.events.on('close', () => {
        debug('emit close event');
        events.emit('close');
    });

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
            .then(() => Promise.all([server.init(), rpc.init()]));
    }

    function addQueue(queueDescriptor) {
        assert(isValidQueueDescriptor(queueDescriptor), 'Invalid queue descriptor (consume)');
        queues.push(queueDescriptor.queue);
    }

    function isValidQueueDescriptor(queueDescriptor) {
        return queueDescriptor instanceof Object && queueDescriptor.queue instanceof Object;
    }

}

