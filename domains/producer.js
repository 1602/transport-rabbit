'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:producer');

module.exports = createProducerFabric;

function createProducerFabric(transport) {

    /**
     * @param spec {Object}:
     *  - exchangeName {String} - name of exchange
     *  - exchangeType {String} - type of exchange
     *  - exchangeOptions {Object} - options for assertExchange
     *  - channelName {String} - name of channel (optional, defaults to 'default')
     */
    return function createProducer(spec) {
        const {
            exchangeName,
            exchangeType = 'direct',
            exchangeOptions = {},
            channelName = 'default'
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

        return function publish(payload, toRoute, opts) {

            chan.assertOpenChannel();

            debug('Sending msg to exchange "%s" via route "%s", corrId=%s', exchangeName, toRoute, opts && opts.correlationId);
            console.log('payload is', payload);

            return chan.publish(
                exchangeName,
                toRoute,
                new Buffer(JSON.stringify({ payload }), 'utf-8'),
                opts
            );
        };

    }

}
