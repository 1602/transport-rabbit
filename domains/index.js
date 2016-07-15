'use strict';

const assert = require('assert');

const createConnection = require('./connection');
const createChannel = require('./channel');
const createRpcClientFabric = require('./rpc/client');
const createRpcServerFabric = require('./rpc/server');
const createClientFabric = require('./client');
const createRouterFabric = require('./router');
const createProducerFabric = require('./producer');
const createConsumerFabric = require('./consumer');
const createPubsubFabric = require('./pubsub');
const createCommandFabric = require('./command');
const queue = require('./queue');
const debug = require('debug')('rabbit:transport');

module.exports = initTransport;

function initTransport(settings) {

    const channels = Object.create(null);

    const EventEmitter = require('events');
    const events = new EventEmitter();

    const transport = {
        events,
        getReady: () => new Promise(resolve => events.on('ready', resolve)),
        close: () => {
            return connection.close();
        },
        addChannel,
        getChannel: name => getChannel(name),
        isConnected: () => connection.isConnected(),
        assertedQueues: Object.create(null),
    };

    function getChannel(name) {
        const channelName = name || 'default';
        const channel = channels[channelName];
        assert(channel, `Channel ${channelName} does not exist`);
        return channel;
    }

    const connection = createConnection(settings);

    transport.connection = connection;
    addChannel('default');
    transport.queue = queue(transport, 'default');

    transport.client = createClientFabric(transport);
    transport.router = createRouterFabric(transport);
    transport.producer = createProducerFabric(transport);
    transport.consumer = createConsumerFabric(transport);

    transport.rpcClient = createRpcClientFabric(transport, settings);
    transport.rpcServer = createRpcServerFabric(transport, settings);

    const pubsub = createPubsubFabric(transport);
    transport.publisher = pubsub.createPublisher;
    transport.subscriber = pubsub.createSubscriber;

    const command = createCommandFabric(transport);
    transport.createCommandSender = command.createCommandSender;
    transport.createCommandServer = command.createCommandServer;
    transport.createCommandResultRecipient = command.createCommandResultRecipient;

    connection.events.on('connected', () => {
        Promise.all(Object.keys(channels)
            .map(name => {
                debug('init "%s" channel', name);
                const chan = transport.getChannel(name);
                return connection.createChannel()
                    .then(ch => chan.bind(ch, settings));
            }))
            .then(() => transport.events.emit('ready'))
            .catch(err => {
                debug('error during init', err.stack);
                transport.events.emit('error', err);
            });
    });

    connection.events.on('close', () => {
        debug('emit close event');
        events.emit('close');
    });

    // channel.events.on('close', channelErrored => {
    //     if (channelErrored && !connection.isDisconnected()) {
    //         connection.forceClose();
    //     }
    // });

    return transport;

    function addChannel(channelName) {
        channelName = channelName || 'default';

        if (!(channelName in channels)) {
            debug('creating wrapper for %s channel', channelName);
            channels[channelName] = createChannel(channelName);
        }

        return channels[channelName];
    }
}

