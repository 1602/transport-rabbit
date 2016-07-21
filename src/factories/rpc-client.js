'use strict';

const helpers = require('../helpers');
const assert = require('assert');
const debug = require('debug')('rabbit:rpc:client');

const generateId = helpers.generateId;

const DEFAULT_TIMEOUT = 60 * 1000;

module.exports = function createRpcClientFactory(transport) {

    const awaitingResponseHandlers = Object.create(null);

    transport.events.on('close', () => rejectAll(new Error('Transport closed')));

    return function createRpcClient(exchangeName, opts) {

        assert(typeof exchangeName === 'string',
            'RPC client requires exchangeName: String to be specified');

        const {
            channelName,
            defaultTimeout = DEFAULT_TIMEOUT
        } = (opts || {});

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        const consumer = transport.consumer({
            channelName,
            exchangeName,
            queueName: '',
            queueOptions: {
                durable: false,
                exclusive: true,
                autoDelete: true
            },
            consumeOptions: {
                noAck: true
            },
            consume(payload, job) {
                const {
                    correlationId,
                    type
                } = job.msg.properties;

                if (type === 'error') {
                    // TODO (bo) convert to error maybe?
                    rejectHandler(correlationId, payload);
                } else {
                    resolveHandler(correlationId, payload);
                }

            }
        });

        return function send(payload, opts) {
            const {
                timeout = defaultTimeout
            } = (opts || {});

            const handler = addResponseHandler(timeout);
            const route = 'query';

            debug('sending query to "%s" exchange routed as %s',
                exchangeName,
                route);

            assert(consumer.assertedQueue, 'Reply queue is not asserted');

            producer(payload, route, {
                correlationId: handler.correlationId,
                replyTo: consumer.assertedQueue,
                expiration: timeout
            });

            return handler.deferred.promise;

        };
    };

    function addResponseHandler(timeout) {
        const correlationId = generateId();
        const deferred = Promise.defer();
        
        const timer = setTimeout(() =>
            rejectHandler(correlationId, new Error('RPC request expired')), timeout);

        const responseHandler = {
            correlationId,
            timer,
            deferred
        };

        awaitingResponseHandlers[correlationId] = responseHandler;

        return responseHandler;
    }

    function popHandler(correlationId) {
        const handler = awaitingResponseHandlers[correlationId];
        if (handler) {
            delete awaitingResponseHandlers[correlationId];
            clearTimeout(handler.timer);
        }
        return handler;
    }

    function resolveHandler(correlationId, result) {
        const handler = popHandler(correlationId);
        if (!handler) {
            return;
        }
        handler.deferred.resolve(result);
    }

    function rejectHandler(correlationId, err) {
        const handler = popHandler(correlationId);
        if (!handler) {
            return;
        }
        handler.deferred.reject(err);
    }

    function rejectAll(err) {
        Object.keys(awaitingResponseHandlers)
            .forEach(correlationId => rejectHandler(correlationId, err));
    }
};
