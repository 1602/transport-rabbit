'use strict';

module.exports = createServerFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:server');

function createServerFabric(transport) {

    return {
        declareIntermediateServer,
        declareTerminalServer
    };

    function declareIntermediateServer(spec) {
        const {
            consumer,
            producer
        } = spec;

        assert(consumer, 'Server must have consumer specified');
        assert(producer, 'Server must have producer specified');

        consumer.pipeTo({
            result: transport.client({ producer, route: 'result' }),
            error: transport.client({ producer, route: 'error' })
        });
    }

    function declareTerminalServer(spec) {
        assert(spec.consumer, 'Server must have consumer specified');
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

