'use strict';

module.exports = createRpcFabric;

const assert = require('assert');
const debug = require('debug')('rabbit:rpc');
const helpers = require('./helpers');
const generateId = helpers.generateId;
const getQueueName = helpers.getQueueName;

function createRpcFabric(transportLink, channelLink, settings) {

    const transport = transportLink;
    const channel = channelLink;
    const descriptors = [];

    const rpcCallbackQueues = Object.create(null);
    const awaitingResponseHandlers = Object.create(null);
    const awaitingExpiration = [];

    const expirationInterval = setInterval(() => {
        debug('Expire rpc');
        const now = Date.now();
        let expireMe;
        while (awaitingExpiration[0] && awaitingExpiration[0].expireAt <= now) {
            expireMe = awaitingExpiration.shift();
            expireMe.deferred.reject(new Error('Awaiting response handler expired by timeout'));
            delete awaitingResponseHandlers[expireMe.correlationId];
        }
    }, settings.rpcExpirationInterval);

    transportLink.events.on('close', () => clearInterval(expirationInterval));

    return {
        init,
        declare
    };

    function declare(spec) {
        assert(spec.produce, 'Client must have queue to produce msg to specified');
        transportLink.addQueue(spec.produce);

        descriptors.push(spec);

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes[0];
        const requestQueue = getQueueName(spec.produce.queue);

        return function send(payload) {

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


            debug('Sending msg to queue "%s"', requestQueue);

            channel.get().publish(
                exchange,
                route,
                new Buffer(JSON.stringify({ payload })),
                {
                    correlationId,
                    replyTo: [
                        rpcCallbackQueues[requestQueue].reply.queue,
                        rpcCallbackQueues[requestQueue].error.queue
                    ].join('|')
                }
            );

            return deferred.promise;
        };
    }

    function init() {

        return Promise.all(descriptors
            .map(d => {
                const requestQueue = getQueueName(d.produce.queue);

                return Promise.all([
                    transport.queue.assert('', { exclusive: true }),
                    transport.queue.assert('', { exclusive: true })
                ])
                    .then(queues => {
                        const reply = queues[0];
                        const error = queues[1];

                        rpcCallbackQueues[requestQueue] = { reply, error };

                        return Promise.all([
                            consume(reply.queue, 'resolve'),
                            consume(error.queue, 'reject')
                        ]);

                    });
            }));

        function consume(queueName, action) {
            return transportLink.queue.consume(queueName, msg => {
                debug('Received', msg.properties.type, 'to', queueName);
                channel.get().ack(msg);
                const data = JSON.parse(msg.content.toString());
                const corrId = msg.properties.correlationId;
                awaitingResponseHandlers[corrId].deferred[action](data.payload);
                delete awaitingResponseHandlers[corrId];
                awaitingExpiration.splice(awaitingExpiration.indexOf(awaitingResponseHandlers[corrId]), 1);
            });
        }
    }

}

