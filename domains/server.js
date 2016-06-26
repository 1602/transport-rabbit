'use strict';

module.exports = createServerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:server');

function createServerFabric(transportLink, channelLink) {

    const transport = transportLink;
    const channel = channelLink;

    const descriptors = [];

    return {
        init,
        declare
    };

    function init() {

        return Promise.all(descriptors
            .map(d => {
                debug('Will consume', d.consume.queue.exchange, d.consume.queue.routes);
                return Promise.all(d.consume.queue.routes.map(route => {
                    const queueName = d.consume.queue.queueNames[route];

                    return transport.queue.consume(queueName, msg => {
                        debug('Received', (msg.properties.type || 'message'), 'to', queueName);
                        execJob(d.handler[route], msg, d.produce && d.produce.queue.exchange, d.getContextById);
                    })
                        .then(() => debug('Ready to consume queue %s (%s)',
                                                queueName, route));
                }));
            }));
    }

    function declare(spec) {
        assert(spec.consume, 'Server must have queue to consume from specified');
        transport.addQueue(spec.consume);

        descriptors.push(spec);

        if (spec.produce) {
            transport.addQueue(spec.produce);
        }
    }

    function execJob(handler, msg, respondTo, getContextById) {
        const data = JSON.parse(msg.content.toString());
        const ch = channel.get();

        Promise.resolve(getContext())
            .catch(err => debug('Error while retrieving context', err.stack))
            .then(context => handler(data.payload, context))
            .then(payload => reply('result', payload))

            // TODO add external presenter for error
            .catch(error => reply('error', {
                message: String(error.message),
                stack: String(error.stack),
                details: error.details
            }));

        function reply(type, payload) {

            ch.ack(msg);

            const replyTo = parseReplyTo(type, msg.properties.replyTo);
            const correlationId = msg.properties.correlationId;

            if (replyTo) {
                debug('Reply with %s to queue %s', type, replyTo);

                ch.sendToQueue(
                    replyTo,
                    new Buffer(JSON.stringify({ payload })),
                    { correlationId, type }
                );
            } else if (respondTo) {
                debug('Reply with %s to exchange %s', type, respondTo);

                ch.publish(
                    respondTo,
                    type,
                    new Buffer(JSON.stringify({ payload })),
                    { correlationId, type }
                );
            }
        }

        function parseReplyTo(type, replyTo) {
            const method = type === 'result' ? 'shift' : 'pop';
            return replyTo && replyTo.split('|')[method]();
        }

        function getContext() {
            if ('function' !== typeof getContextById) {
                return null;
            }

            if (!msg.properties.correlationId) {
                return null;
            }

            return getContextById(msg.properties.correlationId);
        }
    }
}

