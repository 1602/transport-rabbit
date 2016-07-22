'use strict';

const assert = require('assert');

module.exports = function(transport) {

    return {
        createPublisher,
        createSubscriber
    };

    function createPublisher(exchangeName) {
        assert(typeof exchangeName === 'string',
            'Receiver requires exchangeName: String');

        const route = 'default';

        const produce = transport.producer({
            exchangeName,
            exchangeType: 'fanout'
        });

        return function publish(payload, opts) {
            produce(payload, route, opts);
        };
    }

    function createSubscriber(exchangeName, consume) {
        assert(typeof exchangeName === 'string',
            'Receiver requires exchangeName: String');
        assert(typeof consume === 'function',
            'Receiver requires exchangeName: Function/2');

        return transport.consumer({
            exchangeName,
            exchangeType: 'fanout',
            queueName: '',
            routes: [ 'default' ],
            queueOptions: {
                exclusive: true,
                durable: false,
                autoDelete: true
            },
            consume
        });
    }

};

