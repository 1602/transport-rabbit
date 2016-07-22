'use strict';

module.exports = createConsumerFactory;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFactory(transport) {

    /**
     * @param spec {Object}:
     * @param spec.queueName {String} - name of queue
     * @param spec.queueOptions {Object} - options for assertQueue (defaults to {})
     * @param spec.exchangeName {String} name of exchange
     * @param spec.exchangeType {String} (direct) type of exchange
     * @param spec.exchangeOptions {Object} options for assertExchange
     * @param spec.routes {Array<String>} ([]) routing keys for queue binding
     * @param spec.handler {(Object, { msg, context, ack, nack }) => Promise} - message handler
     * @param spec.consumeOptions {Object} - options for ch.consume (defaults to {})
     * @param spec.channelName {String} - name of channel (optional, defaults to 'default')
     */
    return function createConsumer(spec) {

        let assertedQueue = null;
        let consumerTag = null;

        const {
            queueName, // required, can be empty string for exclusive queue
            exchangeName,
            exchangeType = 'direct',
            exchangeOptions = {},
            routes = [],
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

        const channel = transport.channel(channelName);
        
        const destroy = transport.addInit(init);

        return {
            get assertedQueue() {
                return assertedQueue;
            },
            get consumerTag() {
                return consumerTag;
            },
            cancel
        };
        
        function init() {
            return Promise.resolve()
                .then(() => assertExchange())
                .then(() => assertQueue())
                .then(() => bindQueue())
                .then(() => channel.consume(assertedQueue, handler, consumeOptions))
                .then(res => consumerTag = res.consumerTag)
                .then(() => debug('ready to consume "%s" via %s channel',
                    assertedQueue, channelName));
        }

        function assertExchange() {
            return channel.assertExchange(
                exchangeName,
                exchangeType,
                exchangeOptions);
        }

        function assertQueue() {
            return channel.assertQueue(queueName, queueOptions)
                .then(res => assertedQueue = res.queue);
        }
        
        function bindQueue() {
            if (routes.length) {
                const promises = routes.map(route =>
                    channel.bindQueue(assertedQueue, exchangeName, route));
                return Promise.all(promises);
            }
            // If no routes provided, bind queue to exchange directly
            // (route === queueName)
            // this is useful with RPC pattern (exclusive queues with generated names)
            return channel.bindQueue(
                assertedQueue,
                exchangeName,
                assertedQueue);
        }
        
        function cancel() {
            return channel.cancel(consumerTag)
                .then(() => destroy());
        }

        function handler(msg) {
            if (msg == null) {
                // consume is cancelled, corner case
                return;
            }
            debug(`received ${msg.properties.type || 'msg'} to ${queueName || 'exclusive queue'}`);
            try {
                const data = JSON.parse(msg.content.toString()) || {};
                const {
                    payload,
                    context
                } = data;
                consume(payload, createJob(msg, context));
            } catch (e) {
                console.warn('Malformed message is dropped from queue');
                debug(`dropping ${msg.content.toString()} due to ${e.message}`);
                channel.nack(msg, false, false);
            }
        }

        // TODO extract this
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

            function ack(allUpTo) {
                if (ackStatus) {
                    return;
                }
                ackStatus = 'ack';
                channel.ack(msg, allUpTo);
            }

            function nack(allUpTo, requeue) {
                if (ackStatus) {
                    return;
                }
                ackStatus = 'nack';
                channel.nack(msg, allUpTo, requeue);
            }
        }

    };
}

