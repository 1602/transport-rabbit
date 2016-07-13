'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:client');

module.exports = createClientFabric;

function createClientFabric(transportLink) {

    const transport = transportLink;

    const descriptors = [];

    return {
        declare
    };

    function declare(spec) {
        assert(spec.produce, 'Client must have queue to produce to specified');
        transport.addQueue(spec.produce);

        descriptors.push(spec);

        const exchange = spec.produce.queue.exchange;
        const route = spec.produce.queue.routes && spec.produce.queue.routes[0];

        return function send(payload, toRoute, opts) {
            const chan = transport.getChannel(spec.produce.channel);

            chan.assertOpenChannel();

            return Promise.resolve(getCorrelationId(opts && opts.context))
                .then(correlationId => {
                    debug('Sending msg to route', toRoute || route, 'corrId =', correlationId);

                    return chan.publish(
                        exchange,
                        toRoute || route,
                        new Buffer(JSON.stringify({ payload }), 'utf-8'),
                        { correlationId }
                    );
                });
        };

        function getCorrelationId(context) {
            if (context && spec.getContextId) {
                return spec.getContextId(context);
            }

            return null;
        }
    }

}
