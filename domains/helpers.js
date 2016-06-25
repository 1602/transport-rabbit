'use strict';

module.exports = {
    generateId,
    getQueueName,
};

function generateId() {
    return Math.random().toString(36).substr(2) +
        Math.random().toString(36).substr(1) +
        Math.random().toString(36).substr(1);
}

function getQueueName(queueDescriptor) {
    const exchange = queueDescriptor.exchange;
    const route = queueDescriptor.routes[0];
    return [ exchange, route ].join('.');
}

