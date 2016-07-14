'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:producer');

module.exports = createProducerFabric;

function createProducerFabric(transport) {

    return {
        declare
    };

    /**
     * @param spec {Object}:
     *  - exchangeName {String} - name of exchange.
     *  - exchangeType {String} - type of exchange.
     *  - exchangeOptions {Object} - options for exchange assertion.
     *  - channelName {String} - name of channel (optional, defaults to 'default').
     */
    function declare(spec) {
        const {
            exchangeName,
            exchangeType,
            exchangeOptions,
            channelName
        } = spec;

        const chan = transport.addChannel(channelName);

        assert(exchangeName, 'Producer must have exchangeName specified');

        chan.addSetup(() => {
            debug('assert exchange %s type=%s', exchangeName, exchangeType);
            return chan.assertExchange(
                    exchangeName,
                    exchangeType || 'direct',
                    exchangeOptions
                );
        });

        return function send(payload, toRoute, opts) {

            chan.assertOpenChannel();

            debug('Sending msg to route', toRoute, 'corrId =', opts && opts.correlationId);

            return chan.publish(
                exchangeName,
                toRoute,
                new Buffer(JSON.stringify({ payload }), 'utf-8'),
                opts
            );
        };

    }

}
