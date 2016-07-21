'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:command');

module.exports = function createCommandFactory(transport) {

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, opts) {

        const {
            channelName,
            route = 'command'
        } = (opts || {});

        const channel = transport.getChannel(channelName);

        channel.addBinding(() => {
            return channel.bindQueue(
                exchangeName + '.command',
                exchangeName,
                route
            );
        });
        
        const produce = transport.producer({
            exchangeName,
            exchangeType: 'direct'
        });
        
        return function sendCommand(payload, opts) {
            return produce(payload, route, opts)
        };

    }

    function createCommandServer(exchangeName, opts) {
        assert.equal(typeof exchangeName, 'string',
            'Command server requires exchangeName: String to be specified');

        assert.equal(typeof opts, 'object',
            'Command server requires opts: Object to be specified');

        const {
            channelName,
            handler,
            route = 'command'
        } = opts;

        assert.equal(typeof handler, 'function',
            'Command server requires opts.handler: Function/2 to be specified');

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        return transport.consumer({
            channelName,
            exchangeName,
            exchangeType: 'direct',
            queueName: exchangeName + '.command',
            queueOptions: {
                exclusive: false,
                durable: true,
                autoDelete: false
            },
            routingPatterns: [ route ],
            consume(payload, job) {
                const producerOpts = {
                    context: job.context
                };
                Promise.resolve()
                    .then(() => handler(payload, job))
                    .then(result => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            producer(result, 'result', producerOpts);
                        }
                    }, err => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            producer({
                                message: err.message,
                                stack: err.stack,
                                details: err.details
                            }, 'error', producerOpts);
                        }
                    });
            }
        });

    }

    function createCommandResultRecipient(exchangeName, opts) {
        assert(opts, 'Required "opts" argument is missing');

        const {
            result,
            error,
            channelName
        } = opts;

        createConsumer('result', result);
        createConsumer('error', error);

        function createConsumer(type, handler) {
            transport.consumer({
                channelName,
                exchangeName,
                queueName: [ exchangeName, type ].join('.'),
                routingPatterns: [ type ],
                consumeOptions: { noAck: true },
                consume(payload, job) {
                    handler(payload, job);
                }
            });
        }

    }

};

