'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFabric(transport) {

    let assertedQueueName = '';

    return {
        declare
    };

    function declare(spec) {
        const {
            channelName = 'default',
            queueName, // required, can be empty string for exclusive queue
            exchangeName,
            routingPatterns,
            queueOptions,
            consumerOptions,
            consume,
        } = spec;

        const noAck = consumerOptions && consumerOptions.noAck;

        assert(typeof queueName !== 'undefined',
            'Consumer must have queue to consume from specified');

        assert(typeof consume === 'function',
            'Consumer must have "consume(payload, job)" function specified');

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
                            )
                                .then(() => {
                                    debug('queue "%s" bound to "%s" routed as "%s"',
                                        asserted.queue,
                                        exchangeName,
                                        routingPattern);
                                });
                        });
                    } else if (queueName === '') {
                        // bind exclusive queues to exchanges to be able to use
                        // producer(payload, generatedQueue);
                        channel.bindQueue(
                            asserted.queue,
                            exchangeName,
                            asserted.queue
                        );
                    }

                    assertedQueueName = asserted.queue;

                    return channel.consume(asserted.queue, handler, consumerOptions)
                        .then(() => debug('ready to consume "%s" via %s channel',
                              asserted.queue, channelName));
                });
        });

        return {
            get assertedQueue() {
                return assertedQueueName;
            }
        };

        function handler(msg) {

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

            consume(data && data.payload, { msg, ack, nack });

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

        }

    }
}

