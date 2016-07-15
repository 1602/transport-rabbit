'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:command');

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

        const channel = transport.getChannel(channelName);

        channel.addBinding(() => {
            return channel.bindQueue(
                exchangeName + '.command',
                exchangeName,
                'command'
            );
        });

        return transport.client({
            channelName,
            exchangeName,
            exchangeType: 'direct',
            getContextId,
            route: 'command'
        });

    }

    function createCommandServer(exchangeName, opts) {
        assert.equal(typeof exchangeName, 'string',
            'Command server requires exchangeName: String to be specified');

        assert.equal(typeof opts, 'object',
            'Command server requires opts: Object to be specified');

        const {
            channelName,
            handler
        } = opts;

        assert.equal(typeof handler, 'function',
            'Command server requires opts.handler: Function/2 to be specified');

        let producer = null;

        if (opts.produceResults !== false) {
            producer = transport.producer({
                channelName,
                exchangeName
            });
        }

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
            routes: [ 'command' ],
            consume(payload, job) {
                if (!producer) {
                    try {
                        handler(payload, job);
                    } catch(err) {
                        // TODO: reconsider
                        console.error(err);
                    }
                    return;
                }
                Promise.resolve()
                    .then(() => handler(payload, job))
                    .then(result => producer(result, 'result'))
                    .catch(err => producer({
                        message: err.message,
                        stack: err.stack,
                        details: err.details
                    }, 'error'));
            }
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

        createConsumer('result', result);
        createConsumer('error', error);

        function createConsumer(type, handler) {
            transport.consumer({
                channelName,
                exchangeName,
                queueName: [ exchangeName, type ].join('.'),
                routingPatterns: [ type ],
                consumerOptions: { noAck: true },
                consume(payload, job) {
                    const {
                        msg,
                        ack,
                        nack
                    } = job;

                    getContext(msg.properties.correlationId)
                        .then(context => {
                            handler(payload, { context, ack, nack });
                        });
                }
            });
        }

        function getContext(correlationId) {
            if ('function' !== typeof getContextById) {
                return Promise.resolve(null);
            }

            if (!correlationId) {
                return Promise.resolve(null);
            }

            return Promise.resolve(getContextById(correlationId))
                .catch(err => {
                    debug('error while retrieving context', err.stack);
                    return null;
                });
        }


    }

};

