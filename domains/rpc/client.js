'use strict';

const helpers = require('../helpers');
const assert = require('assert');
const debug = require('debug')('rabbit:rpc:client');

const generateId = helpers.generateId;

const DEFAULT_TIMEOUT = 60 * 1000;

module.exports = function createRpcClientFabric(transport) {

    const awaitingResponseHandlers = Object.create(null);

    transport.events.on('close', () => rejectAll(new Error('Transport closed')));

    return function createRpcClient(exchangeName, opts) {

        assert(typeof exchangeName === 'string',
            'RPC client requires exchangeName: String to be specified');

        opts = opts || {};

        const {
            channelName
        } = opts;

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
            consumerOptions: {
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
            const handler = addResponseHandler(opts);
            const route = 'query';

            debug('sending query to "%s" exchange routed as %s',
                exchangeName,
                route);

            assert(consumer.assertedQueue, 'Reply queue is not asserted');

            producer(payload, route, {
                correlationId: handler.correlationId,
                replyTo: consumer.assertedQueue,
                expiration: handler.timeout
            });

            return handler.deferred.promise;

        };
    };

    function addResponseHandler(opts) {
        const {
            timeout = DEFAULT_TIMEOUT
        } = (opts || {});
        
        const correlationId = generateId();
        const deferred = Promise.defer();
        
        const timer = setTimeout(() =>
            rejectHandler(correlationId, new Error('RPC request expired')), timeout);

        const responseHandler = {
            correlationId,
            startedAt: Date.now(),
            timeout,
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
