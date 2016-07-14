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
            channelName,
            queueName,
            exchangeName,
            routingPatterns,
            queueOptions,
            consumerOptions,
            getContextById,
            handler,
        } = spec;

        const noAck = consumerOptions && consumerOptions.noAck;

        assert(queueName, 'Consumer must have queue to consume from specified');

        const channel = transport.getChannel(channelName);

        transport.addSetup(() => {

            return channel.assertQueue(queueName, queueOptions)
                .then(asserted => {

                    routingPatterns.forEach(routingPattern => {
                        channel.bindQueue(
                            asserted.queue,
                            exchangeName,
                            routingPattern
                        );
                    });

                    return channel.consume(asserted.queue, consume, consumerOptions)
                        .then(() => debug('ready to consume queue %s via %s',
                              asserted.queue, channelName || 'default'));
                });
        });

        function consume(msg) {

            debug(`received ${msg.properties.type ||
                  'msg'} to ${queueName ||
                  'exclusive queue'} via ${channelName ||
                  'default'}`);

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
                }));

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

