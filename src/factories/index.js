'use strict';

const createChannelFactory = require('./channel');
const createProducerFactory = require('./producer');
const createConsumerFactory = require('./consumer');
const createCommandFactory = require('./command');
const createRpcClientFactory = require('./rpc-client');
const createRpcServerFactory = require('./rpc-server');
const createPubsubFactory = require('./pubsub');

module.exports = function createFactories(transport) {

    const pubsubFactory = createPubsubFactory(transport);
    const commandFactory = createCommandFactory(transport);
    
    return {
        channel: createChannelFactory(transport),

        producer: createProducerFactory(transport),
        consumer: createConsumerFactory(transport),

        rpcClient: createRpcClientFactory(transport),
        rpcServer: createRpcServerFactory(transport),

        publisher: pubsubFactory.createPublisher,
        subscriber: pubsubFactory.createSubscriber,

        commandSender: commandFactory.createCommandSender,
        commandServer: commandFactory.createCommandServer,
        commandResultRecipient: commandFactory.createCommandResultRecipient
    };
    
};
