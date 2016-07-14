'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:client');

module.exports = createClientFabric;

function createClientFabric(transport) {

    return {
        declare
    };

    /**
     * @param spec {Object}
     *  - getContextId
     *  - producer
     */
    function declare(spec) {
        const {
            getContextId,
            channelName,
            producer,
            route
        } = spec;

        assert(producer, 'Client must have producer specified');

        const channel = transport.addChannel(channelName);

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
    }

}
