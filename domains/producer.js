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
            exchangeType = 'direct',
            exchangeOptions,
            channelName
        } = spec;

        const chan = transport.addChannel(channelName);

        assert(exchangeName, 'Producer must have exchangeName specified');

        chan.addSetup(() => {
            debug('assert exchange %s type=%s', exchangeName, exchangeType);
            return chan.assertExchange(
                exchangeName,
                exchangeType,
                exchangeOptions
            );
        });

        return function send(payload, route, opts) {

            chan.assertOpenChannel();

            debug('sending msg to exchange "%s" via route "%s", corrId=%s', exchangeName, route, opts && opts.correlationId);

            console.log('payload is', payload);

            return chan.publish(
                exchangeName,
                route,
                new Buffer(JSON.stringify({ payload }), 'utf-8'),
                opts
            );
        };

    }

}
