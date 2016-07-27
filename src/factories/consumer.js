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
        assert.equal(typeof spec, 'object',
            'Consumer requires spec: Object');

        let consumerTag = null;

        const {
            queueName,
            consume,
            consumeOptions = {},
            channelName = 'default',
        } = spec;

        assert(queueName, 'Consumer requires queueName: String');

        assert.equal(typeof consume, 'function',
            'Consumer requires consume: Function/2');

        const channel = transport.channel(channelName);

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.consume(queueName, handler, consumeOptions))
                .then(res => consumerTag = res.consumerTag)
                .then(() => debug('ready to consume "%s" via %s channel',
                    queueName, channelName));
        });

        return {
            get consumerTag() {
                return consumerTag;
            },
            cancel
        };

        function cancel() {
            return channel.cancel(consumerTag);
        }

        function handler(msg) {
            if (msg == null) {
                // consume is cancelled, corner case
                return;
            }
            debug(`received ${msg.properties.type || 'msg'} to ${queueName}`);
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

        function createJob(msg, context) {

            let ackStatus = consumeOptions.noAck === true ? 'ack' : null;

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

