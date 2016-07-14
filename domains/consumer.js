'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFabric(transport) {

    return {
        declare
    };

    function declare(spec) {
        const {
            channelName = 'default',
            queueName,
            exchangeName,
            routingPatterns,
            queueOptions,
            consumerOptions,
            getContextById,
            handler,
        } = spec;

        const noAck = consumerOptions && consumerOptions.noAck;

        let pipe;

        assert(typeof queueName !== 'undefined',
            'Consumer must have queue to consume from specified');

        const channel = transport.addChannel(channelName);

        channel.addSetup(() => {

            return channel.assertQueue(queueName, queueOptions)
                .then(asserted => {

                    if (routingPatterns) {
                        routingPatterns.forEach(routingPattern => {
                            channel.bindQueue(
                                asserted.queue,
                                exchangeName,
                                routingPattern
                            );
                        });
                    }

                    return channel.consume(asserted.queue, consume, consumerOptions)
                        .then(() => debug('ready to consume queue %s via %s',
                              asserted.queue, channelName));
                });
        });

        return {
            pipeTo: p => pipe = p
        };

        function consume(msg) {

            debug(`received ${
                msg.properties.type || 'msg'
            } to ${
                queueName || 'exclusive queue'
            } via ${
                channelName
            }`);

            let msgHandled = noAck === true;

            const data = JSON.parse(msg.content.toString());
            const ch = channel.get();
            const ack = () => safeAck(true);
            const nack = () => safeAck(false);

            getContext()
                .then(context => handler(data && data.payload, {
                    msg,
                    context,
                    ack,
                    nack
                }))
                .then(res => {
                    ack();
                    if (pipe && pipe.result) {
                        pipe.result(res);
                    }
                })
                .catch(err => {
                    ack();
                    if (pipe && pipe.error) {
                        pipe.error({
                            message: err.message,
                            stack: err.stack,
                            details: err.details,
                        });
                    }
                });

            function safeAck(isAck) {
                if (msgHandled) {
                    return;
                }
                msgHandled = true;

                if (isAck) {
                    debug('ack');
                    return ch.ack(msg);
                }

                debug('nack');
                return ch.nack(msg);
            }

            function getContext() {
                if ('function' !== typeof getContextById) {
                    return Promise.resolve(null);
                }

                const correlationId = msg.properties.correlationId;

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

    }
}

