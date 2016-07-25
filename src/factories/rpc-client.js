'use strict';

const helpers = require('../helpers');
const assert = require('assert');

const generateId = helpers.generateId;

const DEFAULT_TIMEOUT = 60 * 1000;

module.exports = function createRpcClientFactory(transport) {

    const awaitingResponseHandlers = Object.create(null);

    transport.events.on('close', () => rejectAll(new Error('Transport closed')));

    return function createRpcClient(exchangeName, opts) {
        opts = opts || {};

        assert(typeof exchangeName === 'string',
            'RPC client requires exchangeName: String to be specified');

        const {
            channelName = 'default',
            defaultTimeout = DEFAULT_TIMEOUT
        } = opts;
        
        const queueName = exchangeName + '.' + helpers.generateId();

        const channel = transport.channel(channelName);

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        const queueOptions = {
            durable: false,
            exclusive: true,
            autoDelete: true
        };

        const consumeOptions = {
            noAck: true
        };

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(queueName, queueOptions))
                .then(() => channel.bindQueue(queueName, exchangeName, queueName));
        });

        transport.consumer({
            channelName,
            queueName,
            consumeOptions,
            consume
        });

        return function send(payload, opts) {
            const {
                timeout = defaultTimeout
            } = (opts || {});

            const handler = addResponseHandler(timeout);

            producer(payload, 'query', {
                correlationId: handler.correlationId,
                replyTo: queueName,
                expiration: timeout
            });

            return handler.deferred.promise;

        };

        function consume(payload, job) {
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
