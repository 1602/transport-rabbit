'use strict';

const debug = require('debug')('rabbit:client');
const assert = require('assert');

module.exports = createClientFabric;

function createClientFabric(transport) {

    /**
     * @param spec {Object}
     *  - getContextId
     *  - producer
     */
    return function createClient(spec) {
        const {
            channelName,
            exchangeName,
            exchangeType,

            getContextId,
            route
        } = spec;

        assert(exchangeName, 'Client requires exchangeName');

        const producer = transport.producer({
            channelName,
            exchangeName,
            exchangeType
        });

        const channel = transport.assertChannel(channelName);

        return function send(payload, opts) {
            channel.assertOpenChannel();

            return Promise.resolve(getCorrelationId(opts && opts.context))
                .then(correlationId => {
                    debug('Sending msg to route %s corrId=%s', route, correlationId);

                    return producer(payload, route, Object.assign(
                        {},
                        opts,
                        { correlationId }
                    ));
                });
        };

        function getCorrelationId(context) {
            if (context && getContextId) {
                return getContextId(context);
            }

            return null;
        }

    };

}
