'use strict';

const assert = require('assert');

module.exports = function(transport/* , settings */) {

    return function createRpcServer(exchangeName, opts) {

        assert.equal(typeof exchangeName, 'string',
            'RPC server requires exchangeName: String to be specified');
        assert.equal(typeof opts, 'object',
            'RPC server requires opts: Object to be specified');

        const {
            channelName,
            handler
        } = opts;

        assert.equal(typeof handler, 'function',
            'RPC server requires handler: Function/2 to be specified');

        assert.equal(handler.length, 2,
            'RPC server requires handler: Function/2 to be specified');

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        transport.consumer({
            channelName,
            exchangeName,
            queueName: exchangeName + '.query',
            routingPatterns: [ 'query' ],
            consume(payload, job) {
                const {
                    correlationId,
                    replyTo
                } = job.msg.properties;

                Promise.resolve()
                    .then(() => handler(payload, job))
                    .then(res => {
                        job.ack();
                        producer(res, replyTo, {
                            correlationId,
                            type: 'result'
                        });
                    })
                    .catch(err => {
                        job.ack();
                        producer({
                            message: err.message,
                            stack: err.stack,
                            details: err.details
                        }, replyTo, {
                            correlationId,
                            type: 'error'
                        });
                    });
            }
        });
    };

};
