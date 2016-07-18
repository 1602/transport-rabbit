'use strict';

const assert = require('assert');

const createConnection = require('./connection');
const createChannel = require('./channel');
const createRpcClientFabric = require('./rpc/client');
const createRpcServerFabric = require('./rpc/server');
const createProducerFabric = require('./producer');
const createConsumerFabric = require('./consumer');
const createPubsubFabric = require('./pubsub');
const createCommandFabric = require('./command');
const queue = require('./queue');
const debug = require('debug')('rabbit:transport');
const EventEmitter = require('events');

module.exports = initTransport;

function initTransport(settings) {

    const channels = Object.create(null);
    const events = new EventEmitter();

    let ready = false;

    const transport = {
        events,
        getReady,
        close,
        assertChannel,
        getChannel: name => getChannel(name),
        isConnected: () => connection.isConnected(),
        assertedQueues: Object.create(null)
    };

    function getChannel(name) {
        const channelName = name || 'default';
        const channel = channels[channelName];
        assert(channel, `Channel ${channelName} does not exist`);
        return channel;
    }

    function getReady() {
        return new Promise(resolve => {
            if (ready) {
                resolve();
            } else {
                events.once('ready', resolve);
            }
        });
    }
    
    function close() {
        return connection.isConnected() ? connection.close() : Promise.resolve();
    }

    const connection = createConnection(settings);
    assertChannel('default');

    transport.connection = connection;
    transport.queue = queue(transport, 'default');

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
                    .catch(err => {
                        connection.close();
                        throw err;
                    })
                    .then(ch => chan.bind(ch, settings));
            }))
            .then(() => {
                ready = true;
                transport.events.emit('ready');
            })
            .catch(err => {
                debug('error during init', err.stack);
                transport.events.emit('error', err);
            });
    });

    connection.events.on('close', () => {
        debug('emit close event');
        events.emit('close');
    });

    return transport;

    function assertChannel(channelName) {
        channelName = channelName || 'default';

        if (!(channelName in channels)) {
            debug('creating wrapper for %s channel', channelName);
            channels[channelName] = createChannel(channelName);
        }

        return channels[channelName];
    }
}

