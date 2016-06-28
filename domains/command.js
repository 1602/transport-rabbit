'use strict';

module.exports = function createCommandFabric(transportLink) {

    const transport = transportLink;

    return {
        createCommandSender,
        createCommandServer,
        createCommandResultRecipient
    };

    function createCommandSender(exchangeName, conf) {
        return transport.client({
            produce: {
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'direct',
                    routes: [ 'command' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            },
            getContextId: conf && conf.getContextId
        });
    }

    function createCommandServer(exchangeName, handler) {
        transport.server({
            consume: {
                queue: {
                    exchange: exchangeName,
                    exchangeType: 'direct',
                    routes: [ 'command' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            },
            handler: {
                command: handler
            },
            produce: {
                queue: {
                    exchange: exchangeName,
                    routes: [ 'result', 'error' ],
                    options: {
                        exclusive: false,
                        durable: true,
                        autoDelete: false
                    }
                }
            }
        });
    }

    function createCommandResultRecipient(exchangeName, spec) {
        transport.server({
            consume: {
                queue: {
                    exchange: exchangeName,
                    routes: [ 'result', 'error' ],
                }
            },
            handler: {
                result: spec.result,
                error: spec.error,
            },
            getContextById: spec.getContextById
        });
    }

};

