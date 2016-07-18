'use strict';

module.exports = createRpcClientFabric;

const helpers = require('../helpers');
const assert = require('assert');
const debug = require('debug')('rabbit:rpc:client');

const generateId = helpers.generateId;

const DEFAULT_RPC_EXPIRATION_INTERVAL = 1000;

function createRpcClientFabric(transport, settings) {

    const awaitingResponseHandlers = Object.create(null);
    const awaitingExpiration = [];
    const rpcExpirationInterval = settings.rpcExpirationInterval || DEFAULT_RPC_EXPIRATION_INTERVAL;

    let expirationInterval;

    // stop expiration watchdog when transport closes
    transport.events.on('close', stopExpirationInterval);

    function stopExpirationInterval() {
        if (expirationInterval) {
            clearInterval(expirationInterval);
            expirationInterval = null;
        }
    }

    return function createRpcClient(exchangeName, opts) {

        assert(typeof exchangeName === 'string',
            'RPC client requires exchangeName: String to be specified');

        opts = opts || {};

        const {
            channelName
        } = opts;

        // start expiration interval on demand
        expirationInterval = expirationInterval || startExpirationInterval();

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

                const action = {
                    error: 'reject',
                    result: 'resolve'
                }[type];

                awaitingResponseHandlers[correlationId].deferred[action](payload);
                delete awaitingResponseHandlers[correlationId];
                awaitingExpiration.splice(awaitingExpiration.indexOf(
                    awaitingResponseHandlers[correlationId]), 1);
            }
        });

        return function send(payload) {
            const resp = addResponseHandler();
            const route = 'query';

            debug('sending query to "%s" exchange routed as %s',
                exchangeName,
                route);

            assert(consumer.assertedQueue, 'Reply queue is not asserted');

            producer(payload, route, {
                correlationId: resp.correlationId,
                replyTo: consumer.assertedQueue,
                expiration: String(rpcExpirationInterval)
            });

            return resp.promisedResult;

        };
    };

    function startExpirationInterval() {
        debug('Start expiration interval each %d ms', rpcExpirationInterval);
        return setInterval(() => {
            debug('expire rpc: check');
            const now = Date.now();
            let expireMe;
            while (awaitingExpiration[0] && awaitingExpiration[0].expireAt <= now) {
                debug('expire rpc: expire handler by timeout');
                expireMe = awaitingExpiration.shift();
                expireMe.deferred.reject(new Error('Awaiting response handler expired by timeout'));
                delete awaitingResponseHandlers[expireMe.correlationId];
                if (awaitingExpiration.length === 0) {
                    stopExpirationInterval();
                }
            }
        }, rpcExpirationInterval);
    }

    function addResponseHandler() {
        const correlationId = generateId();
        const deferred = Promise.defer();

        const responseHandler = {
            correlationId,
            deferred,
            startedAt: Date.now()
        };

        if (settings.rpcTimeout) {
            responseHandler.expireAt = responseHandler.startedAt + settings.rpcTimeout;
            awaitingExpiration.push(responseHandler);
        }

        awaitingResponseHandlers[correlationId] = responseHandler;

        return {
            promisedResult: deferred.promise,
            correlationId
        };
    }
}
