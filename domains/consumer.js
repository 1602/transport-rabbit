'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');
const createJob = require('./job');

function createConsumerFabric(transport) {

    /**
     * @param spec {Object}:
     *  - queueName {String} - name of queue
     *  - exchangeName {String} - name of exchange to bind queue to
     *  - routingPatterns {Array<String>} - routing keys for queue binding
     *    (optional, defaults to [])
     *  - handler {(Object, { msg, context, ack, nack }) => Promise} - message handler
     *  - queueOptions {Object} - options for assertQueue (defaults to {})
     *  - consumerOptions {Object} - options for ch.consume (defaults to {})
     *  - channelName {String} - name of channel (optional, defaults to 'default')
     */
    return function createConsumer(spec) {

        let assertedQueueName = '';

        const {
            queueName, // required, can be empty string for exclusive queue
            exchangeName,
            routingPatterns = [],
            queueOptions = {},
            consumerOptions = {},
            consume,
            channelName = 'default',
        } = spec;

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
                .then(() => channel.consume(
                    assertedQueueName,
                    handler,
                    consumerOptions
                ))
                .then(() => debug('ready to consume "%s" via %s channel',
                      assertedQueueName, channelName)));
        });

        return {
            get assertedQueue() {
                return assertedQueueName;
            }
        };

        function handler(msg) {
            debug(`received ${msg.properties.type || 'msg'} to ${queueName || 'exclusive queue'}`);
            const data = JSON.parse(msg.content.toString());
            consume(data && data.payload, createJob(msg, channel, consumerOptions));
        }

    };
}

