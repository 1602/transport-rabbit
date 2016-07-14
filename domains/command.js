'use strict';

const assert = require('assert');

module.exports = function createCommandFabric(transport) {

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, opts) {

        opts = opts || {};

        const {
            channelName,
            getContextId
        } = opts;

        const producer = transport.producer({
            channelName,
            exchangeName,
            exchangeType: 'direct',
        });

        const channel = transport.getChannel(channelName);
        channel.addSetup(() => {
            return channel.bindQueue(
                exchangeName + '.command',
                exchangeName,
                'command'
            );
        });

        return transport.client({
            producer,
            getContextId,
            route: 'command'
        });

    }

    function createCommandServer(exchangeName, handler, opts) {
        opts = opts || {};

        const {
            channelName
        } = opts;

        const consumer = transport.consumer({
            channelName,
            exchangeName,
            exchangeType: 'direct',
            queueName: exchangeName + '.command',
            queueOptions: {
                exclusive: false,
                durable: true,
                autoDelete: false
            },
            routes: [ 'command' ],
            handler
        });

        if (opts.produceResults === false) {
            return transport.terminalServer({ consumer });
        }

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        return transport.intermediateServer({
            consumer,
            producer
        });

    }

    function createCommandResultRecipient(exchangeName, opts) {
        assert(opts, 'Required "opts" argument is missing');

        const {
            result,
            error,
            channelName,
            getContextById
        } = opts;

        consume('result', result);
        consume('error', error);

        function consume(type, handler) {
            const consumer = transport.consumer({
                channelName,
                exchangeName,
                queueName: [ exchangeName, type ].join('.'),
                routingPatterns: [ type ],
                consumerOptions: { noAck: true },
                getContextById,
                handler
            });

            transport.terminalServer({ consumer });
        }

    }

};

