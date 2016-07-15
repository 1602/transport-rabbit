'use strict';

const expect = require('expect');
const queueTransport = require('../');
const rabbitUrl = process.env.RABBIT_URL || 'amqp://192.168.99.101:5672';

// mimic ORM
const jobs = [];

function Job(data) {
    jobs.push(Object.assign(this, data, { id: jobs.length + 1 }));
}

Job.create = data => Promise.resolve(new Job(data));
Job.find = id => {
    const job = jobs.find(j => j.id === Number(id));
    return Promise.resolve(job);
};

describe('server', () => {

    context('normal flow', () => {

        let transport;
        let client;
        let result1;
        let context1;
        let result2;
        let context2;

        before(() => {
            transport = queueTransport({ url: rabbitUrl });

            client = transport.createCommandSender('task', {
                getContextId: context => Job.create({ context })
                    .then(job => String(job.id))
            });

            transport.createCommandResultRecipient('task', {

                getContextById: contextId => Job.find(contextId)
                    .then(job => job.context),

                error: (err, job) => {
                    result2 = err;
                    context2 = job.context;
                },

                result: (res, job) => {
                    result1 = res;
                    context1 = job.context;
                }

            });

            transport.createCommandServer('task', {
                handler(msg, job) {
                    job.ack();

                    if (msg) {
                        return 'hola';
                    }

                    throw new Error('Oops');
                }
            });

            return transport.getReady();

        });

        after(() => transport.close());

        it('can produce results asyncronously', (done) => {
            client(1, { context: { say: 'hello' } });
            setTimeout(() => {
                expect(result1).toEqual('hola');
                expect(context1).toEqual({ say: 'hello' });
                done();
            }, 300);
        });

        it('can produce errors asyncronously', (done) => {
            client(0);
            setTimeout(() => {
                expect(result2.message).toEqual('Oops');
                expect(context2).toEqual(null);
                done();
            }, 300);
        });

        it('should continue to work when context not found', (done) => {
            const find = Job.find;
            Job.find = () => Promise.resolve(null);
            result1 = null;
            context1 = 'something';
            client(1, null, { context: { a: 1 } });
            setTimeout(() => {
                Job.find = find;
                expect(result1).toEqual('hola');
                expect(context1).toEqual(null);
                done();
            }, 300);
        });

    });

    describe('ack', () => {

        let transport = null;
        let send;
        let handler;

        beforeEach(() => {
            transport = queueTransport({ url: rabbitUrl });
            send = transport.createCommandSender('task-donotcare');

            transport.createCommandServer('task-donotcare', {
                handler: function() {
                    return handler.apply(null, [].slice.call(arguments));
                }
            });

            return transport.getReady();
        });

        afterEach(() => {
            return transport.close();
        });

        it('should allow to nack', done => {
            let rejectedOnce = false;
            handler = (param, job) => {
                expect(param).toBe('hello');
                expect(typeof job.ack).toBe('function');
                expect(typeof job.nack).toBe('function');
                if (rejectedOnce) {
                    job.ack();
                    expect(job.ack).toNotThrow();
                    done();
                    return;
                }
                rejectedOnce = true;
                job.nack();
            };
            send('hello');
        });
    });

});

