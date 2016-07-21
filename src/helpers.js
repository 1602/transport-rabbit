'use strict';

module.exports = {
    generateId
};

function generateId() {
    return Math.random().toString(36).substr(2) +
        Math.random().toString(36).substr(1) +
        Math.random().toString(36).substr(1);
}

