'use strict';

module.exports = function(channelLink) {

    const channel = channelLink;

    const queue = {
        assert: (queueName, options) =>
            channel.get().assertQueue(queueName, options),

        delete: (queueName, options) =>
            channel.get().deleteQueue(queueName, options),

        check: queueName =>
            channel.get().checkQueue(queueName),

        purge: queueName =>
            channel.get().purgeQueue(queueName),

        messageCount: queueName =>
            queue.check(queueName)
                .then(check => check.messageCount),

        consumerCount: queueName =>
            queue.check(queueName)
                .then(check => check.consumerCount),

        consume: (queueName, fn) =>
            channel.get().consume(queueName, msg => {
                if (msg !== null) {
                    fn(msg);
                }
            })
    };

    return queue;

};
