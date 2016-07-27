'use strict';

const assert = require('assert');
const debug = require('debug')('rabbit:producer');

module.exports = function createProducerFactory(transport) {

    /**
     * @param spec {Object}
     * @param spec.exchangeName {String} exchange to produce to
     * @param spec.channelName {String} (default) channel name
     */
    return function createProducer(spec) {

        const {
            exchangeName,
            channelName = 'default'
        } = spec;

        assert(exchangeName, 'Producer must have exchangeName specified');

        const channel = transport.channel(channelName);

        return function publish(payload, route, opts) {
            opts = opts || {};

            const context = opts.context;

            debug('publish to exchange "%s" via route "%s"', exchangeName, route);

            const buffer = new Buffer(JSON.stringify({ payload, context }), 'utf-8');

            return Promise.resolve()
                .then(() => channel.publish(exchangeName, route, buffer, opts));
        };

    };

};
