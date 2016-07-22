'use strict';

const assert = require('assert');

module.exports = function(transport) {

    return {
        createPublisher,
        createSubscriber
    };

    function createPublisher(exchangeName, channelName = 'default') {
        assert.equal(typeof exchangeName, 'string',
            'Publisher requires exchangeName: String');
        
        const channel = transport.channel(channelName);
        
        transport.addInit(() => channel.assertExchange(exchangeName, 'topic'));

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
            channelName = 'default'
        } = spec;
        
        const queueOptions = {
            exclusive: true,
            durable: false,
            autoDelete: true
        };

        const consumeOptions = {
            noAck: true
        };
        
        const channel = transport.channel(channelName);
        
        let queueName = null;
        let consumer = null;
        
        transport.addInit(() => {
            return channel.assertQueue('', queueOptions)
                .then(res => queueName = res.queue)
                .then(() => channel.bindQueue(queueName, exchangeName, topic))
                .then(() => {
                    consumer = transport.consumer({
                        queueName,
                        consume,
                        consumeOptions
                    }); 
                });
        });
        
        return  {
            get consumer() {
                return consumer;
            }
        };
    }

};

