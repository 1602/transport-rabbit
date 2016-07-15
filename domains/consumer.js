'use strict';

module.exports = createConsumerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFabric(transport) {

    let assertedQueueName = '';

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

        const channel = transport.addChannel(channelName);
        const noAck = consumerOptions.noAck;

        channel.addSetup(() => {
            return channel.assertQueue(queueName, queueOptions)
                .then(asserted => {

                    console.log('routingPatterns', routingPatterns);
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

                    if (queueName === '') {
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

    };
}

