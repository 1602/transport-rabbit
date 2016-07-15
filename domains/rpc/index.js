'use strict';

module.exports = createRpcFabric;

const createRpcClientFabric = require('./client');
const createRpcServerFabric = require('./server');

function createRpcFabric(transport, settings) {

    const declareClient = createRpcClientFabric(transport, settings);
    const declareServer = createRpcServerFabric(transport, settings);

    return {
        declareClient,
        declareServer
    };

}

