const axios = require('axios');
const WebSocket = require('ws');
const { getAxiosProxyConfig, getWebSocketProxyOptions } = require('./proxy');

class BrowserbaseService {
    constructor() {
        this.sessionId = null;
        this.sessionUrl = null;
        this.agentStream = null;
        this.wsConnection = null;
        this.messageId = 1;
        this.pendingCommands = new Map();
    }

    async createSession() {
        try {
            const response = await axios.post(
                'https://gemini.browserbase.com/api/session',
                { timezone: 'HKT' },
                {
                    ...getAxiosProxyConfig(),
                    headers: {
                        'Content-Type': 'application/json'
                    }
                }
            );

            const data = response.data;
            if (!data.success) {
                throw new Error('Failed to create Browserbase session: success=false');
            }

            this.sessionId = data.sessionId;
            this.sessionUrl = data.sessionUrl;

            const wsMatch = data.sessionUrl.match(/wss=([^&]+)/);
            const wsUrl = wsMatch ? decodeURIComponent(wsMatch[1]) : null;

            console.log(`[Browserbase] Session created: ${this.sessionId}`);
            console.log(`[Browserbase] Session URL: ${this.sessionUrl}`);

            return {
                sessionId: this.sessionId,
                sessionUrl: this.sessionUrl,
                wsUrl
            };
        } catch (error) {
            console.error('[Browserbase] Failed to create session:', error.message);
            if (error.response) {
                console.error('[Browserbase] Response status:', error.response.status);
                console.error('[Browserbase] Response data:', error.response.data);
            }
            throw error;
        }
    }

    async sendAgentGoal(goal) {
        if (!this.sessionId) {
            throw new Error('Session has not been created. Call createSession() first.');
        }

        try {
            const encodedGoal = encodeURIComponent(goal);
            const model = encodeURIComponent('google/gemini-3-flash-preview');
            const url = `https://gemini.browserbase.com/api/agent/stream?sessionId=${this.sessionId}&goal=${encodedGoal}&model=${model}`;

            console.log('[Browserbase] Sending agent goal...');
            console.log(`[Browserbase] Goal: ${goal.substring(0, 100)}...`);

            const response = await axios.get(url, {
                ...getAxiosProxyConfig(),
                responseType: 'stream'
            });

            console.log('[Browserbase] Agent event stream started');

            const stream = response.data;
            this.agentStream = stream;
            stream.on('error', (streamError) => {
                console.error('[Browserbase] Agent event stream error:', streamError.message);
            });
            stream.on('close', () => {
                if (this.agentStream === stream) {
                    this.agentStream = null;
                }
            });

            let streamClosed = false;
            const closeStream = () => {
                if (streamClosed) {
                    return;
                }

                streamClosed = true;
                stream.destroy();
                if (this.agentStream === stream) {
                    this.agentStream = null;
                }
            };

            stream.once('data', () => {
                setTimeout(closeStream, 250);
            });

            setTimeout(closeStream, 2000);
            stream.resume();

            return stream;
        } catch (error) {
            console.error('[Browserbase] Failed to send agent goal:', error.message);
            throw error;
        }
    }

    normalizeWsUrl(wsUrl) {
        if (!wsUrl) {
            return '';
        }

        const decodedUrl = decodeURIComponent(wsUrl);
        if (decodedUrl.startsWith('wss://') || decodedUrl.startsWith('ws://')) {
            return decodedUrl;
        }

        return `wss://${decodedUrl}`;
    }

    sendCDPCommand(method, params = {}) {
        return new Promise((resolve, reject) => {
            if (!this.wsConnection || this.wsConnection.readyState !== WebSocket.OPEN) {
                reject(new Error('WebSocket is not connected'));
                return;
            }

            const id = this.messageId++;
            const message = JSON.stringify({ id, method, params });
            const timeoutId = setTimeout(() => {
                this.pendingCommands.delete(id);
                reject(new Error('CDP command timed out'));
            }, 5000);

            this.pendingCommands.set(id, { resolve, reject, timeoutId });

            try {
                this.wsConnection.send(message);
            } catch (error) {
                clearTimeout(timeoutId);
                this.pendingCommands.delete(id);
                reject(error);
            }
        });
    }

    clearPendingCommands(reason = 'CDP connection closed') {
        for (const [id, pending] of this.pendingCommands.entries()) {
            clearTimeout(pending.timeoutId);
            pending.reject(new Error(reason));
            this.pendingCommands.delete(id);
        }
    }

    async getTargets() {
        const result = await this.sendCDPCommand('Target.getTargets');
        return Array.isArray(result?.targetInfos) ? result.targetInfos : [];
    }

    connectToCDP(wsUrl, options = {}) {
        return new Promise((resolve, reject) => {
            const {
                targetKeyword,
                targetMatcher,
                targetLabel,
                onUrlChange,
                onTargetReached,
                timeout = 1800000,
                pollInterval = 3000
            } = options;
            const reconnectDelay = 500;
            const staleReconnectMs = 12000;
            const targetDescription = targetLabel || targetKeyword || 'target page';
            const fullWsUrl = this.normalizeWsUrl(wsUrl);

            console.log(`[Browserbase] Connecting to CDP: ${fullWsUrl.substring(0, 60)}...`);

            let settled = false;
            let pollTimer = null;
            let reconnectTimer = null;
            let activeSocket = null;
            const targetUrls = new Map();
            let lastUrlChangeAt = Date.now();
            let lastReconnectAt = 0;
            let pollInFlight = false;
            let hasLoggedConnectionReady = false;

            const stopPolling = () => {
                if (pollTimer) {
                    clearInterval(pollTimer);
                    pollTimer = null;
                }
            };

            const clearSocketRef = (socket) => {
                if (activeSocket === socket) {
                    activeSocket = null;
                }

                if (this.wsConnection === socket) {
                    this.wsConnection = null;
                }
            };

            const closeSocket = (socket, reason = 'CDP connection closed') => {
                if (!socket) {
                    return;
                }

                socket.__intentionalClose = true;
                this.clearPendingCommands(reason);
                clearSocketRef(socket);

                if (socket.readyState === WebSocket.CONNECTING || socket.readyState === WebSocket.OPEN) {
                    socket.close();
                }
            };

            const cleanup = () => {
                clearTimeout(timeoutId);
                stopPolling();
                if (reconnectTimer) {
                    clearTimeout(reconnectTimer);
                    reconnectTimer = null;
                }
                closeSocket(activeSocket);
            };

            const settleResolve = (value) => {
                if (settled) {
                    return;
                }

                settled = true;
                cleanup();
                resolve(value);
            };

            const settleReject = (error) => {
                if (settled) {
                    return;
                }

                settled = true;
                cleanup();
                reject(error);
            };

            const timeoutId = setTimeout(() => {
                settleReject(new Error('CDP connection timed out'));
            }, timeout);

            const scheduleReconnect = (reason) => {
                if (settled || reconnectTimer) {
                    return;
                }

                lastReconnectAt = Date.now();
                reconnectTimer = setTimeout(() => {
                    reconnectTimer = null;
                    if (settled) {
                        return;
                    }

                    stopPolling();
                    closeSocket(activeSocket, `CDP reconnect: ${reason}`);
                    connect();
                }, reconnectDelay);
            };

            const resolveWithUrl = (currentUrl) => {
                if (onTargetReached) {
                    const result = onTargetReached(currentUrl);
                    settleResolve(result || currentUrl);
                    return;
                }

                settleResolve(currentUrl);
            };

            const isTargetUrl = (currentUrl) => {
                if (!currentUrl) {
                    return false;
                }

                if (typeof targetMatcher === 'function') {
                    return targetMatcher(currentUrl);
                }

                if (targetKeyword) {
                    return currentUrl.includes(targetKeyword);
                }

                return false;
            };

            const handleObservedUrl = (currentUrl) => {
                lastUrlChangeAt = Date.now();
                console.log(`[Browserbase] URL changed: ${currentUrl}`);

                if (onUrlChange) {
                    onUrlChange(currentUrl);
                }

                if (isTargetUrl(currentUrl)) {
                    console.log(`[Browserbase] Reached ${targetDescription}`);
                    resolveWithUrl(currentUrl);
                    return true;
                }

                return false;
            };

            const observeTargetUrl = (targetKey, currentUrl) => {
                if (!currentUrl || currentUrl === 'about:blank') {
                    return false;
                }

                if (targetUrls.get(targetKey) === currentUrl) {
                    return false;
                }

                targetUrls.set(targetKey, currentUrl);
                return handleObservedUrl(currentUrl);
            };

            const pollTargets = async () => {
                const socket = activeSocket;
                if (pollInFlight || settled || !socket || socket.readyState !== WebSocket.OPEN) {
                    return;
                }

                pollInFlight = true;
                try {
                    let sawNewUrl = false;
                    const targets = await this.getTargets();

                    for (const target of targets) {
                        if (target.type && target.type !== 'page') {
                            continue;
                        }

                        const currentUrl = target.url || '';
                        const targetKey = target.targetId || currentUrl;
                        if (observeTargetUrl(targetKey, currentUrl)) {
                            return;
                        }

                        if (currentUrl && currentUrl !== 'about:blank') {
                            sawNewUrl = true;
                        }
                    }

                    if (!sawNewUrl) {
                        const now = Date.now();
                        if (now - lastUrlChangeAt >= staleReconnectMs && now - lastReconnectAt >= staleReconnectMs) {
                            scheduleReconnect('stale page target binding');
                        }
                    }
                } catch (_error) {
                    const now = Date.now();
                    if (now - lastUrlChangeAt >= staleReconnectMs && now - lastReconnectAt >= staleReconnectMs) {
                        scheduleReconnect('target polling stalled');
                    }
                } finally {
                    pollInFlight = false;
                }
            };

            const connect = () => {
                if (settled) {
                    return;
                }

                if (activeSocket && (activeSocket.readyState === WebSocket.CONNECTING || activeSocket.readyState === WebSocket.OPEN)) {
                    return;
                }

                const socket = new WebSocket(fullWsUrl, getWebSocketProxyOptions());
                socket.__intentionalClose = false;
                activeSocket = socket;
                this.wsConnection = socket;

                socket.on('open', () => {
                    if (settled || socket !== activeSocket) {
                        return;
                    }

                    this.messageId = 1;
                    lastReconnectAt = Date.now();
                    socket.send(JSON.stringify({
                        id: this.messageId++,
                        method: 'Target.setDiscoverTargets',
                        params: { discover: true }
                    }));

                    if (!hasLoggedConnectionReady) {
                        console.log('[Browserbase] CDP WebSocket connected');
                        console.log('[Browserbase] Multi-tab URL monitoring enabled');
                        hasLoggedConnectionReady = true;
                    }

                    stopPolling();
                    pollTimer = setInterval(pollTargets, pollInterval);
                    pollTargets();
                });

                socket.on('message', (data) => {
                    if (settled || socket !== activeSocket) {
                        return;
                    }

                    try {
                        const message = JSON.parse(data.toString());

                        if (Object.prototype.hasOwnProperty.call(message, 'id') && this.pendingCommands.has(message.id)) {
                            const pending = this.pendingCommands.get(message.id);
                            clearTimeout(pending.timeoutId);
                            this.pendingCommands.delete(message.id);

                            if (message.error) {
                                pending.reject(new Error(message.error.message || 'CDP command failed'));
                            } else {
                                pending.resolve(message.result);
                            }
                            return;
                        }

                        if (message.method === 'Target.targetCreated' || message.method === 'Target.targetInfoChanged') {
                            const info = message.params?.targetInfo;
                            if (info?.type === 'page') {
                                if (observeTargetUrl(info.targetId || info.url || 'page', info.url || '')) {
                                    return;
                                }
                                setTimeout(pollTargets, 150);
                            }
                        }
                    } catch (_error) {
                        // Ignore malformed event payloads.
                    }
                });

                socket.on('error', (error) => {
                    if (socket.__intentionalClose || settled || socket !== activeSocket) {
                        return;
                    }

                    console.error('[Browserbase] CDP WebSocket error:', error.message);
                    this.clearPendingCommands(`CDP connection error: ${error.message}`);
                    scheduleReconnect('socket error');
                });

                socket.on('unexpected-response', (_request, response) => {
                    if (socket.__intentionalClose || settled || socket !== activeSocket) {
                        return;
                    }

                    const statusCode = response?.statusCode;
                    this.clearPendingCommands(`CDP WebSocket handshake failed: HTTP ${statusCode}`);

                    if (statusCode === 410) {
                        settleReject(new Error('Browserbase session ended before the target page was observed'));
                        return;
                    }

                    scheduleReconnect(`handshake failed: HTTP ${statusCode}`);
                });

                socket.on('close', () => {
                    const wasActive = socket === activeSocket;
                    clearSocketRef(socket);

                    if (socket.__intentionalClose || settled || !wasActive) {
                        return;
                    }

                    stopPolling();
                    this.clearPendingCommands('page websocket closed');
                    scheduleReconnect('socket closed');
                });
            };

            connect();
        });
    }

    disconnect() {
        if (this.agentStream) {
            this.agentStream.destroy();
            this.agentStream = null;
        }

        if (this.wsConnection) {
            this.wsConnection.__intentionalClose = true;
            this.wsConnection.close();
            this.wsConnection = null;
        }
    }
}

module.exports = { BrowserbaseService };
