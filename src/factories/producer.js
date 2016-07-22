'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:producer');

module.exports = function createProducerFactory(transport) {

    /**
     * @param spec {Object}
     * @param spec.exchangeName {String} name of exchange
     * @param spec.exchangeType {String} (direct) type of exchange
     * @param spec.exchangeOptions {Object} options for assertExchange
     * @param spec.channelName {String} (default) name of channel
     */
    return function createProducer(spec) {
        const {
            exchangeName,
            exchangeType = 'direct',
            exchangeOptions = {},
            channelName = 'default'
        } = spec;

        assert(exchangeName, 'Producer must have exchangeName specified');

        const channel = transport.channel(channelName);

        transport.addInit(init);

        return function publish(payload, route, opts) {
            opts = opts || {};
            
            const context = opts.context;

            debug('publish to exchange "%s" via route "%s"', exchangeName, route);

            const buffer = new Buffer(JSON.stringify({ payload, context }), 'utf-8');

            return Promise.resolve()
                .then(() => channel.publish(exchangeName, route, buffer, opts));
        };
        
        function init() {
            return channel.assertExchange(
                exchangeName,
                exchangeType,
                exchangeOptions);
        }

    };

};
