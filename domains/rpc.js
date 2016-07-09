'use strict';

module.exports = createRpcFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:rpc');
const helpers = require('./helpers');
const generateId = helpers.generateId;
const getQueueName = helpers.getQueueName;

const DEFAULT_RPC_EXPIRATION_INTERVAL = 1000;

function createRpcFabric(transportLink, settings) {

    const transport = transportLink;
    const descriptors = [];

    const rpcCallbackQueues = Object.create(null);
    const awaitingResponseHandlers = Object.create(null);
    const awaitingExpiration = [];

    const rpcExpirationInterval = settings.rpcExpirationInterval || DEFAULT_RPC_EXPIRATION_INTERVAL;
    let expirationInterval;

    function startExpirationInterval() {
        debug('Start expiration interval each %d ms', rpcExpirationInterval);
        return setInterval(() => {
            debug('Expire rpc');
            const now = Date.now();
            let expireMe;
            while (awaitingExpiration[0] && awaitingExpiration[0].expireAt <= now) {
                expireMe = awaitingExpiration.shift();
                expireMe.deferred.reject(new Error('Awaiting response handler expired by timeout'));
                delete awaitingResponseHandlers[expireMe.correlationId];
            }
        }, rpcExpirationInterval);
    }

    transport.events.on('close', () => {
        if (expirationInterval) {
            clearInterval(expirationInterval);
        }
    });

    return {
        init,
        declare
    };

    function declare(spec) {
        expirationInterval = expirationInterval || startExpirationInterval();
        assert(spec.produce, 'Client must have queue to produce msg to specified');
        transport.addQueue(spec.produce);

        descriptors.push(spec);

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes[0];
        const requestQueue = getQueueName(spec.produce.queue);

        return function send(payload) {
            const resp = addResponseHandler();

            debug('Sending msg to queue "%s"', requestQueue);

            // TODO handle false return of Channel#publish (wait for 'drain')
            // Channel#publish mimics the stream.Writable interface in its return
            // value; it will return false if the channel's write buffer is
            // 'full', and true otherwise. If it returns false, it will emit a
            // 'drain' event at some later time.
            transport
                .getChannel(spec.produce.channel)
                .publish(
                    exchange,
                    route,
                    new Buffer(JSON.stringify({ payload })), {
                        correlationId: resp.correlationId,
                        replyTo: [
                            rpcCallbackQueues[requestQueue].reply.queue,
                            rpcCallbackQueues[requestQueue].error.queue
                        ].join('|'),
                        expiration: String(rpcExpirationInterval)
                    });

            return resp.promisedResult;

        };
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

    function init() {

        return Promise.all(descriptors
            .map(d => {
                const requestQueue = getQueueName(d.produce.queue);
                const ch = transport.getChannel(d.produce.channel);

                return Promise.all([
                    ch.assertQueue('', { exclusive: true }),
                    ch.assertQueue('', { exclusive: true })
                ])
                    .then(queues => {
                        const reply = queues[0];
                        const error = queues[1];

                        rpcCallbackQueues[requestQueue] = { reply, error };

                        return Promise.all([
                            consume(ch, reply.queue, 'resolve'),
                            consume(ch, error.queue, 'reject')
                        ]);

                    });
            }));

        function consume(channel, queueName, action) {
            return channel.consume(queueName, msg => {
                debug('Received', msg && msg.properties.type, 'to', queueName);
                const data = JSON.parse(msg.content.toString());
                const corrId = msg && msg.properties.correlationId;
                awaitingResponseHandlers[corrId].deferred[action](data.payload);
                delete awaitingResponseHandlers[corrId];
                awaitingExpiration.splice(awaitingExpiration.indexOf(awaitingResponseHandlers[corrId]), 1);
            }, { noAck: true });
        }
    }

}

