'use strict';

const assert = require('assert');

module.exports = function createRpcServerFactory(transport) {

    return function createRpcServer(exchangeName, spec) {

        assert.equal(typeof exchangeName, 'string',
            'RPC server requires exchangeName: String');
        assert.equal(typeof spec, 'object',
            'RPC server requires opts: Object');

        const {
            channelName = 'default',
            handler,
            queueOptions = {},
            consumeOptions = {}
        } = spec;

        assert.equal(typeof handler, 'function',
            'RPC server requires handler: Function/2');

        const channel = transport.channel(channelName);

        const queueName = exchangeName + '.query';

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(queueName, queueOptions))
                .then(() => channel.bindQueue(queueName, exchangeName, 'query'));
        });

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        return transport.consumer({
            channelName,
            queueName,
            consume,
            consumeOptions
        });

        function consume(payload, job) {
            const {
                correlationId,
                replyTo
            } = job.msg.properties;

            Promise.resolve()
                .then(() => handler(payload, job))
                // TODO think of factoring it away into job
                .then(res => {
                    if (job.ackStatus !== 'nack') {
                        job.ack();
                        producer(res, replyTo, {
                            correlationId,
                            type: 'result'
                        });
                    }
                }, err => {
                    if (job.ackStatus !== 'nack') {
                        job.ack();
                        producer({
                            message: err.message,
                            stack: err.stack,
                            details: err.details
                        }, replyTo, {
                            correlationId,
                            type: 'error'
                        });
                    }
                });
        }
    };

};
