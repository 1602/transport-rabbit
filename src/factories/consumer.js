'use strict';

module.exports = createConsumerFactory;

const assert = require('assert');
const debug = require('debug')('rabbit:consumer');

function createConsumerFactory(transport) {

    /**
     * @param spec {Object}
     * @param spec.queueName {String} name of queue
     * @param spec.consume {(Object, { msg, context, ack, nack }) => Promise} message handler
     * @param spec.consumeOptions {Object} options for consume
     * @param spec.channelName {String} (default) name of channel
     */
    return function createConsumer(spec) {

        let consumerTag = null;

        const {
            queueName, // required, can be empty string for exclusive queue
            consume,
            consumeOptions = {},
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
            get consumerTag() {
                return consumerTag;
            },
            cancel
        };
        
        function init() {
            return Promise.resolve()
                .then(() => channel.consume(queueName, handler, consumeOptions))
                .then(res => consumerTag = res.consumerTag)
                .then(() => debug('ready to consume "%s" via %s channel',
                    queueName, channelName));
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

