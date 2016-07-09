'use strict';

// const debug = require('debug')('rabbit:queue');

module.exports = function(transport, channelName) {

    const queue = {

        assert: (queueName, options) =>
            amqpChannel().assertQueue(queueName, options),

        delete: (queueName, options) =>
            amqpChannel().deleteQueue(queueName, options),

        check: queueName =>
            amqpChannel().checkQueue(queueName),

        consume: (queueName, fn, options) =>
            amqpChannel().consume(queueName, fn, options),

        purge: queueName =>
            amqpChannel().purgeQueue(queueName),

        messageCount: queueName =>
            queue.check(queueName)
                .then(check => check.messageCount),

        consumerCount: queueName =>
            queue.check(queueName)
                .then(check => check.consumerCount),

    };

    return queue;

    function amqpChannel() {
        return transport.getChannel(channelName);
    }

};
