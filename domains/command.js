'use strict';

const assert = require('assert');

module.exports = function createCommandFabric(transportLink) {

    const transport = transportLink;

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, opts) {

        opts = opts || {};

        return transport.client({
            produce: {
                channel: opts.channel,
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'direct',
                    routes: [ 'command' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            },
            getContextId: opts.getContextId
        });
    }

    function createCommandServer(exchangeName, handler, opts) {
        opts = opts || {};
        const schema = {
            consume: {
                channel: opts.channel,
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'direct',
                    routes: [ 'command' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            },
            handler: {
                command: handler
            },
            produce: {
                channel: opts.channel,
                queue: {
                    exchange: exchangeName,
                    routes: [ 'result', 'error' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            }
        };

        if (opts.produceResults === false) {
            delete schema.produce;
        }

        return transport.intermediateServer(schema);
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
                routingPatterns: [ type ],
                consumerOptions: { noAck: true },
                getContextById,
                handler
            });

            transport.terminalServer({ consumer });
        }

    }

};

