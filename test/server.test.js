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

    let transport;
    let client;
    let result1;
    let result2;

    before(() => {
        transport = queueTransport({ url: rabbitUrl });

        client = transport.client({
            produce: {
                queue: {
                    exchange: 'task',
                    routes: [ 'command' ]
                }
            },
            getContextId: context => Job.create({ context }).then(job => String(job.id))
        });

        transport.server({
            consume: {
                queue: {
                    exchange: 'task',
                    routes: [ 'command' ],
                }
            },
            handler: {
                command: msg => {
                    if (msg) {
                        return 'hola';
                    }
                    throw new Error('Oops');
                }
            },
            produce: {
                queue: {
                    exchange: 'subtask',
                    routes: [ 'result', 'error' ]
                }
            }
        });

        transport.server({
            consume: {
                queue: {
                    exchange: 'subtask',
                    routes: [ 'result', 'error' ],
                }
            },
            handler: {
                result: res => {
                    console.log('result handler called');
                    result1 = res;
                },
                error: err => {
                    console.log('error handler called');
                    result2 = err;
                }
            },
            getContextById: contextId => Job.find(contextId).then(job => job && job.context)
        });

        return transport.getReady();

    });

    after(() => transport.close());

    it('can produce results asyncronously', (done) => {
        client(1, null, { context: { say: 'hello' }});
        setTimeout(() => {
            expect(result1).toEqual('hola');
            done();
        }, 300);
    });

    it('can produce errors asyncronously', (done) => {
        client(0);
        setTimeout(() => {
            expect(result2.message).toEqual('Oops');
            done();
        }, 500);
    });

});

