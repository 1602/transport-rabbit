'use strict';

const assert = require('assert');
const helpers = require('../helpers');

module.exports = function(transport) {

    return {
        createPublisher,
        createSubscriber
    };

    function createPublisher(exchangeName, channelName = 'default') {
        assert.equal(typeof exchangeName, 'string',
            'Publisher requires exchangeName: String');

        const channel = transport.channel(channelName);

        channel.addInit(() => channel.assertExchange(exchangeName, 'topic'));

        const produce = transport.producer({
            exchangeName,
            channelName
        });

        return function publish(payload, topic) {
            produce(payload, topic);
        };
    }

    function createSubscriber(exchangeName, spec) {
        assert.equal(typeof exchangeName, 'string',
            'Subscriber requires exchangeName: String');
        assert.equal(typeof spec, 'object',
            'Subscriber requires spec: Object');

        const {
            consume,
            topic = '#',
            channelName = 'default',
            consumeOptions = { noAck: true },
            queueOptions = {
                exclusive: true,
                durable: false,
                autoDelete: true
            }
        } = spec;

        const channel = transport.channel(channelName);
        const queueName = exchangeName + '.' + helpers.generateId();

        channel.addInit(() => {
            return Promise.resolve()
                .then(() => channel.assertQueue(queueName, queueOptions))
                .then(() => channel.bindQueue(queueName, exchangeName, topic));
        });

        return transport.consumer({
            queueName,
            consume,
            consumeOptions
        });
    }

};

