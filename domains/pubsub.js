'use strict';

module.exports = function(transportLink) {

    const transport = transportLink;

    return {
        createBroadcaster,
        createReceiver
    };

    function createBroadcaster(exchangeName) {
        return transport.client({
            produce: {
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'fanout'
                }
            }
        });
    }

    function createReceiver(exchangeName, handler) {
        return transport.server({
            consume: {
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'fanout',
                    routes: [ 'default' ],
                    autogenerateQueues: true,
                    options: {
                        exclusive: true
                    }
                }
            },
            handler: {
                default: handler
            }
        });
    }

};

