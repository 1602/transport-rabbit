'use strict';

module.exports = function(transport) {

    return {
        createBroadcaster,
        createReceiver
    };

    function createBroadcaster(exchangeName) {
        const producer = transport.producer({
            exchangeName,
            exchangeType: 'fanout'
        });

        return transport.client({ producer, route: 'default' });
    }

    function createReceiver(exchangeName, handler) {
        transport.consumer({
            exchangeName,
            exchangeType: 'fanout',
            queueName: '',
            routingPatterns: [ 'default' ],
            queueOptions: {
                exclusive: true
            },
            handler
        });
    }

};

