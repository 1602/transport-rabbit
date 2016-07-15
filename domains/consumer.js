'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFabric(transport) {

    /**
     * @param spec {Object}:
     *  - queueName {String} - name of queue
     *    (optional, defaults to undefined for exclusive queue)
     *  - exchangeName {String} - name of exchange to bind queue to
     *  - routingPatterns {Array<String>} - routing keys for queue binding
     *    (optional, defaults to [])
     *  - handler {(Object, { msg, context, ack, nack }) => Promise} - message handler
     *  - getContextById {String => Promise<Object>} - populates context for handler
     *  - queueOptions {Object} - options for assertQueue (defaults to {})
     *  - consumerOptions {Object} - options for ch.consume (defaults to {})
     *  - channelName {String} - name of channel (optional, defaults to 'default')
     */
    return function createConsumer(spec) {
        const {
            queueName,
            exchangeName,
            routingPatterns = [],
            handler,
            getContextById,
            channelName = 'default',
            queueOptions = {},
            consumerOptions = {},
        } = spec;

        assert(typeof queueName !== 'undefined',
            'Consumer must have queue to consume from specified');

        const channel = transport.addChannel(channelName);
        const noAck = consumerOptions.noAck;

        let pipe;

        channel.addSetup(() => {
            return channel.assertQueue(queueName, queueOptions)
                .then(asserted => {
                    routingPatterns.forEach(routingPattern => {
                        debug(`bind ${asserted.queue} to ${exchangeName} (${routingPattern})`);
                        channel.bindQueue(
                            asserted.queue,
                            exchangeName,
                            routingPattern
                        );
                    });

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

