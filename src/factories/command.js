'use strict';

const assert = require('assert');

module.exports = function createCommandFactory(transport) {

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, opts) {
        assert.equal(typeof exchangeName, 'string',
            'Command sender requires exchangeName: String');

        const commandQueue = exchangeName + '.command';

        const {
            channelName = 'default',
            queueOptions = {}
        } = (opts || {});

        const channel = transport.channel(channelName);

        const produce = transport.producer({
            channelName,
            exchangeName
        });

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(commandQueue, queueOptions))
                .then(() => channel.bindQueue(commandQueue, exchangeName, 'command'));
        });

        return function sendCommand(payload, opts) {
            return produce(payload, 'command', opts);
        };

    }

    function createCommandServer(exchangeName, spec) {
        assert.equal(typeof exchangeName, 'string',
            'Command server requires exchangeName: String');
        assert.equal(typeof spec, 'object',
            'Command server requires spec: Object');

        const {
            channelName = 'default',
            handler,
            produceResults = true,
            queueOptions = {},
            consumeOptions = {}
        } = spec;

        assert.equal(typeof handler, 'function',
            'Command server requires opts.handler: Function/2');

        const commandQueue = exchangeName + '.command';
        const resultQueue = exchangeName + '.result';
        const errorQueue = exchangeName + '.error';

        const channel = transport.channel(channelName);

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(commandQueue, queueOptions))
                .then(() => channel.bindQueue(commandQueue, exchangeName, 'command'));
        });

        if (produceResults) {
            channel.addInit(() => {
                return Promise.resolve()
                    .then(() => channel.assertQueue(resultQueue))
                    .then(() => channel.assertQueue(errorQueue))
                    .then(() => channel.bindQueue(resultQueue, exchangeName, 'result'))
                    .then(() => channel.bindQueue(errorQueue, exchangeName, 'error'));
            });
        }

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        return transport.consumer({
            channelName,
            queueName: commandQueue,
            consume(payload, job) {
                Promise.resolve()
                    .then(() => handler(payload, job))
                    // TODO see if this can be factored away into job
                    .then(result => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            produceResult(result, 'result', job);
                        }
                    }, err => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            produceResult({
                                message: err.message,
                                stack: err.stack,
                                details: err.details
                            }, 'error', job);
                        }
                    });
            },
            consumeOptions
        });

        function produceResult(payload, type, job) {
            if (!produceResults) {
                return;
            }
            return producer(payload, type, { context: job.context });
        }

    }

    function createCommandResultRecipient(exchangeName, opts) {
        assert(opts, 'Required "opts" argument is missing');

        const {
            result,
            error,
            channelName = 'default',
            consumeOptions = { noAck: true }
        } = opts;

        const channel = transport.channel(channelName);

        const resultQueue = exchangeName + '.result';
        const errorQueue = exchangeName + '.error';

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(resultQueue))
                .then(() => channel.assertQueue(errorQueue))
                .then(() => channel.bindQueue(resultQueue, exchangeName, 'result'))
                .then(() => channel.bindQueue(errorQueue, exchangeName, 'error'));
        });

        const resultConsumer = transport.consumer({
            channelName,
            queueName: resultQueue,
            consume: result,
            consumeOptions
        });

        const errorConsumer = transport.consumer({
            channelName,
            queueName: errorQueue,
            consume: error,
            consumeOptions
        });

        return {
            resultConsumer,
            errorConsumer
        };

    }

};

