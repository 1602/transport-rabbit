'use strict';

const assert = require('assert');

module.exports = function createCommandFactory(transport) {
    
    const queueOptions = {
        exclusive: false,
        durable: true,
        autoDelete: false
    };

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, channelName = 'default') {
        assert.equal(typeof exchangeName, 'string',
            'Command sender requires exchangeName: String');

        const commandQueue = exchangeName + '.command';

        const channel = transport.channel(channelName);

        const produce = transport.producer({
            channelName,
            exchangeName
        });

        transport.addInit(function() {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(commandQueue, queueOptions))
                .then(() => channel.bindQueue(commandQueue, exchangeName, 'command'));
        });

        return function sendCommand(payload, opts) {
            return produce(payload, 'command', opts);
        };

    }

    function createCommandServer(exchangeName, opts) {
        assert.equal(typeof exchangeName, 'string',
            'Command server requires exchangeName: String');
        assert.equal(typeof opts, 'object',
            'Command server requires opts: Object');

        const {
            channelName = 'default',
            handler
        } = opts;

        assert.equal(typeof handler, 'function',
            'Command server requires opts.handler: Function/2');

        const commandQueue = exchangeName + '.command';
        const resultQueue = exchangeName + '.result';
        const errorQueue = exchangeName + '.error';

        const channel = transport.channel(channelName);

        transport.addInit(function() {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(commandQueue, queueOptions))
                .then(() => channel.assertQueue(resultQueue, queueOptions))
                .then(() => channel.assertQueue(errorQueue, queueOptions))
                .then(() => channel.bindQueue(commandQueue, exchangeName, 'command'))
                .then(() => channel.bindQueue(resultQueue, exchangeName, 'result'))
                .then(() => channel.bindQueue(errorQueue, exchangeName, 'error'));
        });

        const producer = transport.producer({
            channelName,
            exchangeName
        });

        return transport.consumer({
            channelName,
            queueName: commandQueue,
            consume(payload, job) {
                const producerOpts = {
                    context: job.context
                };
                Promise.resolve()
                    .then(() => handler(payload, job))
                    // TODO see if this can be factored away into job
                    .then(result => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            producer(result, 'result', producerOpts);
                        }
                    }, err => {
                        if (job.ackStatus !== 'nack') {
                            job.ack();
                            producer({
                                message: err.message,
                                stack: err.stack,
                                details: err.details
                            }, 'error', producerOpts);
                        }
                    });
            }
        });

    }

    function createCommandResultRecipient(exchangeName, opts) {
        assert(opts, 'Required "opts" argument is missing');

        const {
            result,
            error,
            channelName = 'default'
        } = opts;

        const channel = transport.channel(channelName);

        const resultQueue = exchangeName + '.result';
        const errorQueue = exchangeName + '.error';

        transport.addInit(function() {
            return Promise.resolve()
                .then(() => channel.assertExchange(exchangeName, 'direct'))
                .then(() => channel.assertQueue(resultQueue, queueOptions))
                .then(() => channel.assertQueue(errorQueue, queueOptions))
                .then(() => channel.bindQueue(resultQueue, exchangeName, 'result'))
                .then(() => channel.bindQueue(errorQueue, exchangeName, 'error'));
        });
        
        const resultConsumer = transport.consumer({
            channelName,
            queueName: resultQueue,
            consume: result,
            consumeOptions: { noAck: true }     // TODO figure if that's safe
        });

        const errorConsumer = transport.consumer({
            channelName,
            queueName: errorQueue,
            consume: error,
            consumeOptions: { noAck: true }
        });
        
        return {
            resultConsumer,
            errorConsumer
        };

    }

};

