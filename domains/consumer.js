'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFabric(transport) {

    /**
     * @param spec {Object}:
     *  - queueName {String} - name of queue
     *  - exchangeName {String} - name of exchange to bind queue to
     *  - routingPatterns {Array<String>} - routing keys for queue binding
     *    (optional, defaults to [])
     *  - handler {(Object, { msg, context, ack, nack }) => Promise} - message handler
     *  - queueOptions {Object} - options for assertQueue (defaults to {})
     *  - consumeOptions {Object} - options for ch.consume (defaults to {})
     *  - channelName {String} - name of channel (optional, defaults to 'default')
     */
    return function createConsumer(spec) {

        let assertedQueueName = '';
        let consumerTag = null;

        const {
            queueName, // required, can be empty string for exclusive queue
            exchangeName,
            routingPatterns = [],
            queueOptions = {},
            consumeOptions = {},
            consume,
            channelName = 'default',
        } = spec;
        
        const noAck = consumeOptions.noAck;

        assert.notEqual(typeof queueName, 'undefined',
            'Consumer must have queue to consume from specified');

        assert.equal(typeof consume, 'function',
            'Consumer must have "consume(payload, job)" function specified');

        const channel = transport.assertChannel(channelName);

        channel.addSetup(() => {
            return channel.assertQueue(queueName, queueOptions)
                .then(asserted => assertedQueueName = asserted.queue)
                .then(() => Promise.all(routingPatterns.map(routingPattern =>
                    channel.bindQueue(
                        assertedQueueName,
                        exchangeName,
                        routingPattern
                    )
                        .then(() => {
                            debug('queue "%s" bound to "%s" routed as "%s"',
                                assertedQueueName,
                                exchangeName,
                                routingPattern);
                        })
                ))
                .then(() => {
                    if (queueName === '') {
                        // bind exclusive queues to exchanges to be able to use
                        // producer(payload, generatedQueue);
                        return channel.bindQueue(
                            assertedQueueName,
                            exchangeName,
                            assertedQueueName
                        );
                    }
                })
                .then(() => channel.consume(assertedQueueName, handler, consumeOptions))
                .then(res => consumerTag = res.consumerTag)
                .then(() => debug('ready to consume "%s" via %s channel',
                      assertedQueueName, channelName)));
        });

        return {
            get assertedQueue() {
                return assertedQueueName;
            },
            get consumerTag() {
                return consumerTag
            },
            cancel() {
                return channel.cancel(consumerTag);
            }
        };

        function handler(msg) {
            if (msg === null) {
                // consume is cancelled
                return;
            }
            debug(`received ${msg.properties.type || 'msg'} to ${queueName || 'exclusive queue'}`);
            const data = JSON.parse(msg.content.toString()) || {};
            const {
                payload,
                context
            } = data;
            consume(payload, createJob(msg, context));
        }

        function createJob(msg, context) {

            let ackStatus = noAck === true ? 'ack' : null;

            return {
                msg,
                ack,
                nack,
                get ackStatus() {
                    return ackStatus;
                },
                context
            };

            function ack() {
                if (ackStatus) {
                    return;
                }
                ackStatus = 'ack';
                channel.ack(msg);
            }

            function nack() {
                if (ackStatus) {
                    return;
                }
                ackStatus = 'nack';
                channel.nack(msg);
            }
        }

    };
}

