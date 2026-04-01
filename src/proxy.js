const { HttpsProxyAgent } = require('https-proxy-agent');
const config = require('./config');

const proxyUrl = (config.proxyUrl || '').trim();
const proxyAgent = proxyUrl ? new HttpsProxyAgent(proxyUrl) : null;

function getAxiosProxyConfig() {
    if (!proxyAgent) {
        return {};
    }

    return {
        proxy: false,
        httpAgent: proxyAgent,
        httpsAgent: proxyAgent
    };
}

function getWebSocketProxyOptions() {
    if (!proxyAgent) {
        return {};
    }

    return {
        agent: proxyAgent
    };
}

module.exports = {
    proxyUrl,
    proxyAgent,
    getAxiosProxyConfig,
    getWebSocketProxyOptions
};
