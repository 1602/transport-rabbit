'use strict';

const assert = require('assert');

module.exports = function(transport) {

    return {
        createBroadcaster,
        createReceiver
    };

    function createBroadcaster(exchangeName) {
        assert(typeof exchangeName === 'string',
            'Receiver requires exchangeName: String');

        return transport.client({
            exchangeName,
            exchangeType: 'fanout',
            route: 'default'
        });
    }

    function createReceiver(exchangeName, consume) {
        assert(typeof exchangeName === 'string',
            'Receiver requires exchangeName: String');
        assert(typeof consume === 'function',
            'Receiver requires exchangeName: Function/2');

        transport.consumer({
            exchangeName,
            exchangeType: 'fanout',
            queueName: '',
            routingPatterns: [ 'default' ],
            queueOptions: {
                exclusive: true
            },
            consume
        });
    }

};

