'use strict';

const queueTransport = require('transport-rabbit');
const rabbitHost = process.env.RABBIT_HOST || '192.168.99.100';

const transport = queueTransport({
    url: `amqp://${rabbitHost}:5672`,
    reconnect: true,
    prefetch: 1,
    quitGracefullyOnTerm: true
});

const resolveCommand = transport.client({
    produce: {
        queue: {
            routes: [ 'command' ],
            exchange: 'url-resolution',
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        },
    },
    getContextId: context => Job.create({ context }).then(job => String(job.id))
});

transport.server({
    consume: {
        queue: {
            routes: [ 'error', 'result' ],
            exchange: 'url-resolution',
            options: {
                exclusive: false,
                durable: true,
                autoDelete: false
            }
        }
    },
    handler: {
        result: (opts, context) => console.log('received result', opts, context),
        error: (err, context) => console.log('received error', err, context)
    },
    getContextById: contextId => Job.find(contextId).then(job => job.context)
});

// put some jobs to the queue
const int = setInterval(() => {
    if (transport.isConnected()) {
        resolveCommand({
            url: 'http://google.com',
            proxy: 'gb'
        }, null, {
            // some arbitrary context
            context: {
                appId: 1,
                opts: { screenshot: true, logs: true},
            }
        });
        setTimeout(() => clearInterval(int), 5000);
    }
}, 1000);

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

