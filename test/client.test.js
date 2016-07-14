'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';
const jobs = [];

/* eslint max-nested-callbacks: [2, 6] */

describe('client', () => {

    let transport;
    let enqueueMessage;

    beforeEach(() => {
        transport = queueTransport({
            url: rabbitUrl
        });

        const producer = transport.producer({
            exchangeName: 'log',
        });

        enqueueMessage = transport.router({
            exchangeName: 'log',
            producer,
            routes: [ 'info', 'warn', 'error' ],
            getContextId: context => Job.create({ context }).then(job => String(job.id))
        });

        return transport.getReady()
            .then(() => Promise.all([
                transport.queue.purge('log.info'),
                transport.queue.purge('log.error')
            ]));
    });

    afterEach(() => transport.close());

    it('should publish message to queue', () => {
        return Promise.resolve(enqueueMessage('Hello world', 'info'))
            .then(result => {
                expect(result).toBe(true);
            })
            .then(() => Promise.all([
                transport.queue.messageCount('log.info'),
                transport.queue.messageCount('log.warn'),
                transport.queue.messageCount('log.error'),
            ]))
            .then(messageCounts => {
                expect(messageCounts[0]).toEqual(1);
                expect(messageCounts[1]).toEqual(0);
                expect(messageCounts[2]).toEqual(0);
            });
    });

    it('should support context', () => {
        return enqueueMessage('log message', 'error', { context: { hello: 'world' } })
            .then(() => expect(jobs.length).toBe(1));
    });

    it('should throw when called to early', () => {
        return transport.close()
            .then(() => {
                transport = queueTransport({ url: rabbitUrl });
                const producer = transport.producer({
                    exchangeName: 'log'
                });

                enqueueMessage = transport.router({
                    producer,
                    exchangeName: 'log',
                    routes: [ 'info', 'warn', 'error' ],
                });

                expect(() => enqueueMessage('hello', 'warn')).toThrow('Client is not connected to channel');
                return transport.getReady();
            });
    });

});

// mimic ORM

function Job(data) {
    jobs.push(Object.assign(this, data, { id: jobs.length + 1 }));
}

Job.create = data => Promise.resolve(new Job(data));
Job.find = id => {
    const job = jobs.find(j => j.id === Number(id));
    return Promise.resolve(job);
};

