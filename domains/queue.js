'use strict';

const debug = require('debug')('rabbit:queue');

module.exports = function(channelWrapper) {

    const queue = {
        assert: (queueName, options) =>
            amqpChannel().assertQueue(queueName, options),

        delete: (queueName, options) =>
            amqpChannel().deleteQueue(queueName, options),

        check: queueName =>
            amqpChannel().checkQueue(queueName),

        purge: queueName =>
            amqpChannel().purgeQueue(queueName),

        bind: (queueName, exchangeName, route, options) => {
            debug(
                'bind "%s" to "%s" exchange using "%s" route',
                queueName,
                exchangeName,
                route);

            return amqpChannel().bindQueue(queueName, exchangeName, route, options);
        },

        messageCount: queueName =>
            queue.check(queueName)
                .then(check => check.messageCount),

        consumerCount: queueName =>
            queue.check(queueName)
                .then(check => check.consumerCount),

        consume: (queueName, fn) =>
            amqpChannel().consume(queueName, msg => {
                if (msg !== null) {
                    fn(msg);
                }
            })
    };

    return queue;

    function amqpChannel() {
        return channelWrapper.get();
    }

};
