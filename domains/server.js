'use strict';

module.exports = createServerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:server');

function createServerFabric(transportLink) {

    const transport = transportLink;
    const descriptors = [];

    return {
        init,
        declare
    };

    // TODO decouple consumer: (channelName, queueName, queueOptions);
    //  - assert queue
    //  - bind exchange
    //  - only one handler which will not return promise
    function init() {

        return Promise.all(descriptors
            .map(d => {
                debug('will attempt to consume %s{%s}',
                    d.consume.queue.exchange,
                    d.consume.queue.routes);

                return Promise.all(d.consume.queue.routes.map(route => {
                    const queueName = d.consume.queue.queueNames[route];

                    const chan = transport.getChannel(d.consume.channel);

                    chan
                        .consume(queueName, msg => {
                            debug(`received ${msg && msg.properties.type || 'msg'} to ${queueName} via ${d.consume.channel || 'default'}`);
                            execJob(
                                chan,
                                d.handler[route],
                                msg,
                                d.produce && d.produce.queue.exchange,
                                d.getContextById,
                                d.consume.options
                            );
                        }, d.consume.options)
                            .then(() => debug('ready to consume queue %s (%s) via %s',
                                  queueName, route, d.consume.channel || 'default'));
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

    function execJob(channel, handler, msg, respondTo, getContextById, consOpts) {
        const data = msg && JSON.parse(msg.content.toString());
        const ch = channel.get();
        let msgHandled = consOpts && consOpts.noAck === true;
        const accept = () => safeAck(true);
        const reject = () => safeAck(false);

        Promise.resolve(getContext())
            .catch(err => debug('Error while retrieving context', err.stack))
            .then(context => handler(data && data.payload, {
                context,
                accept,
                reject
            }))
            .then(payload => reply('result', payload))

            // TODO add external presenter for error
            .catch(error => reply('error', {
                message: String(error.message),
                stack: String(error.stack),
                details: error.details
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

        function reply(type, payload) {

            if (typeof payload === 'undefined') {
                debug('payload is undefined, skip ack and not reply with %s', type);
                return;
            }

            if (!msgHandled) {
                debug('implicit ack');
                ch.ack(msg);
            } else debug('already handled', consOpts);

            const replyTo = parseReplyTo(type, msg && msg.properties.replyTo);
            const correlationId = msg && msg.properties.correlationId;

            if (replyTo) {
                debug('Reply with %s to queue %s', type, replyTo);

                ch.sendToQueue(
                    replyTo,
                    new Buffer(JSON.stringify({ payload }), 'utf-8'),
                    { correlationId, type }
                );
            } else if (respondTo) {
                debug('Reply with %s to exchange %s', type, respondTo);

                ch.publish(
                    respondTo,
                    type,
                    new Buffer(JSON.stringify({ payload }), 'utf-8'),
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

            if (msg && !msg.properties.correlationId) {
                return null;
            }

            return getContextById(msg && msg.properties.correlationId);
        }
    }
}

