'use strict';

module.exports = function(transportLink, channelLink) {

    const transport = transportLink;
    const channel = channelLink;

    return {
        assert: (queueName, options) =>
            channel.get().assertQueue(queueName, options),

        delete: (queueName, options) =>
            channel.get().deleteQueue(queueName, options),

        check: queueName =>
            channel.get().checkQueue(queueName),

        purge: queueName =>
            channel.get().purgeQueue(queueName),

        messageCount: queueName =>
            transport.queue.check(queueName)
                .then(check => check.messageCount),

        consume: (queueName, fn) =>
            channel.get().consume(queueName, msg => {
                if (msg !== null) {
                    fn(msg);
                }
            })
    };

};
