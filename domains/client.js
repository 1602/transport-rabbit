'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:client');

module.exports = createClientFabric;

function createClientFabric() {

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
            producer
        } = spec;

        assert(producer, 'Client must have producer specified');

        return function send(payload, toRoute, opts) {
            return Promise.resolve(getCorrelationId(opts && opts.context))
                .then(correlationId => {
                    debug('Sending msg to route', toRoute, 'corrId =', correlationId);

                    return producer(payload, toRoute, Object.assign({}, opts, {
                        correlationId
                    }));
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
