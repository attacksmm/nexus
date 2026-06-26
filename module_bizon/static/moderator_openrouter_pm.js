(function() {
    'use strict';

    if (window.__nexusBizonModeratorInstalled) return;
    window.__nexusBizonModeratorInstalled = true;

    const DEFAULT_CONFIG = {
        API_BASE: '',
        PROMPT_FILE: '',
        ALT_PROMPT_FILE: '',
        CONTEXT: 4,
        SEC_KEY: '',
        INDIVIDUAL_CHAT: true,
        REPLY_DELAY_MODE: 'hybrid',
        REPLY_DELAY_FIXED_MS: 60000,
        REPLY_DELAY_BASE_MS: 8000,
        REPLY_DELAY_PER_CHAR_MS: 35,
        REPLY_DELAY_PER_WORD_MS: 450,
        REPLY_DELAY_JITTER_MS: 8000,
        REPLY_DELAY_MULTIPLIER: 1,
        REPLY_DELAY_MIN_MS: 60000,
        REPLY_DELAY_MAX_MS: 180000
    };

    const CFG = Object.assign({}, DEFAULT_CONFIG, window.BOT_CONFIG || {});
    const params = new URLSearchParams(window.location.search);
    const secInUrl = params.get('sec');
    const isActive = !!(secInUrl && CFG.SEC_KEY && secInUrl === CFG.SEC_KEY);
    const MAX_RETRY_ATTEMPTS = 2;
    const RETRY_DELAY_MS = 5000;
    const FETCH_TIMEOUT = 180000;
    const MINIMUM_REPLY_DELAY_MS = 60000;

    console.log('[NEXUS-BIZON] Config loaded:', CFG);
    console.log('[NEXUS-BIZON] Status:', { isActive });

    function apiBase() {
        return String(CFG.API_BASE || '').replace(/\/+$/, '');
    }

    function getPromptFile() {
        if (document.querySelector('#files a') && CFG.ALT_PROMPT_FILE) {
            return String(CFG.ALT_PROMPT_FILE).trim();
        }
        return String(CFG.PROMPT_FILE || '').trim();
    }

    function getRoomKey() {
        return `${window.location.host}${window.location.pathname}`;
    }

    function waitForElement(selector, timeoutMs = 60000) {
        return new Promise((resolve, reject) => {
            const el = document.querySelector(selector);
            if (el) return resolve(el);
            const observer = new MutationObserver(() => {
                const found = document.querySelector(selector);
                if (found) {
                    observer.disconnect();
                    resolve(found);
                }
            });
            observer.observe(document.documentElement, { childList: true, subtree: true });
            setTimeout(() => {
                observer.disconnect();
                reject(new Error(`Timeout waiting for ${selector}`));
            }, timeoutMs);
        });
    }

    function postJSON(url, body, timeoutMs = FETCH_TIMEOUT) {
        const controller = new AbortController();
        const timer = setTimeout(() => controller.abort(), timeoutMs);
        return fetch(url, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(body),
            signal: controller.signal
        }).then(async res => {
            clearTimeout(timer);
            let data = null;
            try { data = await res.json(); } catch (_) {}
            if (!res.ok) {
                throw new Error((data && (data.detail || data.error || data.message)) || `HTTP ${res.status}`);
            }
            return data || {};
        }).catch(err => {
            clearTimeout(timer);
            throw err;
        });
    }

    function lookupUser(bizonUserId, message = '') {
        const query = new URLSearchParams({
            user_id: String(bizonUserId || ''),
            room_key: getRoomKey(),
            sec_key: String(CFG.SEC_KEY || '')
        });
        const cleanMessage = String(message || '').trim();
        if (cleanMessage.length >= 80) query.set('message', cleanMessage);
        return fetch(`${apiBase()}/user_lookup?${query.toString()}`)
            .then(res => res.json())
            .then(data => (data && data.client_id) ? data : null)
            .catch(() => null);
    }

    const userMappingCache = new Map();

    function lookupUserCached(bizonUserId, message = '') {
        const key = String(bizonUserId || '').trim();
        if (!key) return Promise.resolve(null);
        if (!userMappingCache.has(key)) {
            userMappingCache.set(key, lookupUser(key, message).then(mapping => {
                if (!mapping) userMappingCache.delete(key);
                return mapping;
            }));
        }
        return userMappingCache.get(key);
    }

    function lookupUserFresh(bizonUserId, message = '') {
        const key = String(bizonUserId || '').trim();
        if (!key) return Promise.resolve(null);
        return lookupUser(key, message).then(mapping => {
            if (mapping) userMappingCache.set(key, Promise.resolve(mapping));
            else userMappingCache.delete(key);
            return mapping;
        });
    }

    function removeVisualMappingArtifacts() {
        document.getElementById('nexus-bizon-mapping-style')?.remove();
        document.getElementById('nexus-bizon-platform-panel')?.remove();
        document.querySelectorAll?.('.nexus-bizon-utm')?.forEach(node => node.remove());
    }

    function annotateAllKnownUsers() {
        removeVisualMappingArtifacts();
    }

    function annotateKnownUser() {
        removeVisualMappingArtifacts();
    }

    function refreshModerationPlatformPanel() {
        removeVisualMappingArtifacts();
        return Promise.resolve([]);
    }

    function startModerationPlatformPanel() {
        removeVisualMappingArtifacts();
    }

    function toNumber(value, fallback) {
        const num = Number(value);
        return Number.isFinite(num) ? num : fallback;
    }

    function clamp(value, min, max) {
        return Math.min(Math.max(value, min), max);
    }

    function countWords(text) {
        const normalized = String(text || '').trim();
        if (!normalized) return 0;
        return normalized.split(/\s+/).filter(Boolean).length;
    }

    function calculateReplyDelayMs(answer) {
        const mode = String(CFG.REPLY_DELAY_MODE || 'range').trim().toLowerCase();
        if (['off', 'none', 'disabled'].includes(mode)) return 0;

        const text = String(answer || '').trim();
        const chars = text.length;
        const words = countWords(text);
        const fixed = toNumber(CFG.REPLY_DELAY_FIXED_MS, 0);
        const base = toNumber(CFG.REPLY_DELAY_BASE_MS, 0);
        const perChar = toNumber(CFG.REPLY_DELAY_PER_CHAR_MS, 0);
        const perWord = toNumber(CFG.REPLY_DELAY_PER_WORD_MS, 0);
        const jitter = Math.max(0, toNumber(CFG.REPLY_DELAY_JITTER_MS, 0));
        const multiplier = toNumber(CFG.REPLY_DELAY_MULTIPLIER, 1);
        const minMs = Math.max(MINIMUM_REPLY_DELAY_MS, toNumber(CFG.REPLY_DELAY_MIN_MS, MINIMUM_REPLY_DELAY_MS));
        const maxMs = Math.max(minMs, toNumber(CFG.REPLY_DELAY_MAX_MS, 0));

        let value = fixed;
        if (mode === 'chars') value = base + perChar * chars;
        else if (mode === 'words') value = base + perWord * words;
        else if (mode === 'hybrid') value = base + perChar * chars + perWord * words;
        else if (mode === 'range' || mode === 'random') value = minMs + Math.random() * (maxMs - minMs);
        value = value * multiplier;
        if (jitter > 0 && mode !== 'range' && mode !== 'random') value += Math.random() * jitter;
        value = Math.round(clamp(value, minMs, maxMs));
        return value;
    }

    const NO_REPLY_PHRASES = new Set([
        'ок','окей','okay','ok','спс','спасибо','благодарю','thanks','thx','+','++','+1',
        'да','нет','угу','ага','понятно','ясно','хорошо','класс','круто','супер',
        'привет','здравствуйте','добрый день','добрый вечер','всем привет',
        'слышно','видно','не слышу','не слышно','нет звука','звука нет','нет сигнала',
        'все отлично','всё отлично','все хорошо','всё хорошо','все ок','всё ок',
        'отлично','прекрасно','все в норме','всё в норме','тут','здесь',
        'спасибо большое','спасибо вам','спасибо за тему','полезная тема','тема очень полезная',
        'я с вами','я с вами, рада','рада','жду','когти','всегда','очень'
    ]);

    function isNoiseMessage(text, userName = '') {
        const normalized = String(text || '').replace(/\u00a0/g, ' ').trim().replace(/\s+/g, ' ');
        if (!normalized) return true;
        const lower = normalized.toLowerCase();
        const plain = lower.replace(/[.!?,;:()]+$/g, '').trim();
        if (NO_REPLY_PHRASES.has(lower) || NO_REPLY_PHRASES.has(plain)) return true;
        const shortSegments = lower.split(/[.!?,;:+]+/).map(part => part.trim()).filter(Boolean);
        if (shortSegments.length > 1 && shortSegments.every(part => NO_REPLY_PHRASES.has(part))) return true;
        const normalizedName = String(userName || '').replace(/\u00a0/g, ' ').trim().replace(/\s+/g, ' ').toLowerCase();
        const firstName = normalizedName.split(' ')[0] || '';
        if (normalizedName && (plain === normalizedName || (firstName.length >= 2 && plain === firstName))) return true;
        if (/^[+\-=_*/\\|~`.,!?;:()\[\]{}<>\s0-9]+$/.test(normalized)) return true;
        if (/^[+*xх×ж,\s]+$/i.test(normalized) && normalized.length <= 32) return true;
        if (/^[\p{Extended_Pictographic}\s]+$/u.test(normalized)) return true;
        if (/^(.)\1{2,}$/u.test(lower)) return true;
        if (/^(да|нет|ок|ага|угу|спасибо|привет|здравствуйте|добрый день|добрый вечер)[.!?…+\s]*$/i.test(normalized)) return true;
        if (/^(?:здравствуйте[,.!+\s]*)?(?:не слышу|не слышно|нет звука|звука нет|не видно|ничего не видно и не слышно)[.!?+\s]*$/i.test(normalized)) return true;
        if (/^(?:хорошо|отлично|прекрасно|все|всё)[\s,.!+]*(?:слышно|видно|слышно и видно|в норме|хорошо|ок)[).!+\s]*$/i.test(normalized)) return true;
        return false;
    }

    function validateConfig() {
        if (!apiBase()) {
            console.error('[NEXUS-BIZON] API_BASE is required');
            return false;
        }
        if (!getPromptFile()) {
            console.error('[NEXUS-BIZON] PROMPT_FILE is required');
            return false;
        }
        return true;
    }

    if (!isActive) {
        console.log('[NEXUS-BIZON] Quiet mode: mapping visitor ids');
        startModerationPlatformPanel();
        const pageParams = new URLSearchParams(window.location.search);
        const utmTerm = pageParams.get('utm_term') || pageParams.get('utm') || pageParams.get('vk_user_id') || '';
        const threadId = pageParams.get('thread_id') || pageParams.get('conversation_id') || pageParams.get('param2') || pageParams.get('param1') || '';

        if (utmTerm && validateConfig()) {
            let mappingSent = false;
            let mappingInFlight = false;
            let mappingAttempts = 0;
            const registerUser = (bizonUserId) => {
                if (mappingSent || !bizonUserId) return true;
                if (mappingInFlight) return false;
                mappingInFlight = true;
                mappingAttempts += 1;
                postJSON(`${apiBase()}/process2`, {
                    userId: bizonUserId,
                    clientId: utmTerm,
                    assistant_id: getPromptFile(),
                    thread_id: threadId || '',
                    room_key: getRoomKey()
                }).then(() => {
                    mappingSent = true;
                    console.log('[NEXUS-BIZON] Mapping registered', { bizonUserId, clientId: utmTerm, threadId, attempts: mappingAttempts });
                }).catch(error => {
                    console.error('[NEXUS-BIZON] Mapping error, retry scheduled:', error);
                    if (mappingAttempts < 10) setTimeout(() => registerUser(bizonUserId), Math.min(15000, 2000 * mappingAttempts));
                }).finally(() => {
                    mappingInFlight = false;
                });
                return true;
            };

            waitForElement('#partnerDetect', 20000).then(pd => {
                if (mappingSent) return;
                const checkSrc = () => {
                    const src = pd.getAttribute('src');
                    const match = src ? src.match(/chatUserId=([a-zA-Z0-9_-]+)/) : null;
                    if (match && match[1]) registerUser(match[1]);
                };
                checkSrc();
                if (!mappingSent) new MutationObserver(checkSrc).observe(pd, { attributes: true, attributeFilter: ['src'] });
            }).catch(() => {});

            const checkGlobal = setInterval(() => {
                if (mappingSent) {
                    clearInterval(checkGlobal);
                    return;
                }
                try {
                    const uid = window.chatUserId
                        || (window.room && window.room.chatUserId)
                        || (window.bznUser && window.bznUser.chatUserId)
                        || (window.bizonUser && window.bizonUser.chatUserId)
                        || (window.webinarData && window.webinarData.chatUserId);
                    if (uid) {
                        registerUser(uid);
                        clearInterval(checkGlobal);
                    }
                } catch (_) {}
            }, 1000);
            setTimeout(() => clearInterval(checkGlobal), 60000);

            waitForElement('#chatframe, ul#chat', 30000).then(chatEl => {
                if (mappingSent) return;
                let pendingOwnText = '';
                const normalizeMappingText = value => String(value || '').replace(/\u00a0/g, ' ').trim().replace(/\s+/g, ' ');
                const checkOwnMsgs = () => {
                    const ownMsgs = chatEl.querySelectorAll('.msg.my[data-userid], .msg_my[data-userid], .msg.viewer.my[data-userid]');
                    for (const msg of ownMsgs) {
                        const uid = msg.getAttribute('data-userid');
                        if (uid) {
                            registerUser(uid);
                            return true;
                        }
                    }
                    if (pendingOwnText) {
                        const candidates = Array.from(chatEl.querySelectorAll('[data-userid]')).reverse();
                        for (const msg of candidates) {
                            const body = msg.querySelector('span.msgbody, .msgbody');
                            if (!body || normalizeMappingText(body.textContent) !== pendingOwnText) continue;
                            const uid = msg.getAttribute('data-userid');
                            if (uid) {
                                registerUser(uid);
                                return true;
                            }
                        }
                    }
                    return false;
                };
                const input = document.getElementById('inputmsg');
                const sendButton = document.querySelector('.input-group-addon.btn-addon .btn-primary');
                const captureOwnText = () => {
                    const text = normalizeMappingText(input && input.value);
                    if (!text) return;
                    pendingOwnText = text;
                    setTimeout(checkOwnMsgs, 100);
                    setTimeout(checkOwnMsgs, 500);
                    setTimeout(checkOwnMsgs, 1500);
                };
                if (sendButton) sendButton.addEventListener('click', captureOwnText, true);
                if (input) input.addEventListener('keydown', event => {
                    if (event.key === 'Enter' && !event.shiftKey) captureOwnText();
                }, true);
                if (checkOwnMsgs()) return;
                const observer = new MutationObserver(() => {
                    if (mappingSent) {
                        observer.disconnect();
                        return;
                    }
                    checkOwnMsgs();
                });
                observer.observe(chatEl, { childList: true, subtree: true });
            }).catch(() => {});
        }
        return;
    }

    if (!validateConfig()) return;
    console.log('[NEXUS-BIZON] Active moderator mode');

    const answeredMessageIds = new Set();
    const queuedMessageIds = new Set();
    const processingMessageIds = new Set();
    const messageRetryCount = new Map();
    const latestMessageIdByUser = new Map();
    let pendingMessages = [];
    const runningUsers = new Set();

    function sleep(ms) {
        return new Promise(resolve => setTimeout(resolve, Math.max(0, Number(ms) || 0)));
    }

    async function resolveUserMapping(userId, message = '') {
        let mapping = await lookupUserCached(userId, message);
        if (mapping) return mapping;
        for (let attempt = 0; attempt < 30; attempt += 1) {
            await sleep(1000);
            mapping = await lookupUserFresh(userId, message);
            if (mapping) return mapping;
        }
        return null;
    }

    function latestMessageIdForUser(userId) {
        return latestMessageIdByUser.get(String(userId || '')) || '';
    }

    function isLatestMessageForUser(item) {
        return !!item && !!item.userId && String(latestMessageIdForUser(item.userId)) === String(item.messageId || '');
    }

    function enqueueMessage(item) {
        if (!item || !item.messageId) return;
        const userKey = String(item.userId || '');
        latestMessageIdByUser.set(userKey, item.messageId);
        if (answeredMessageIds.has(item.messageId) || queuedMessageIds.has(item.messageId) || processingMessageIds.has(item.messageId)) return;
        pendingMessages = pendingMessages.filter(existing => {
            if (!existing || String(existing.userId || '') !== userKey || String(existing.messageId || '') === String(item.messageId)) return true;
            queuedMessageIds.delete(existing.messageId);
            answeredMessageIds.add(existing.messageId);
            return false;
        });
        queuedMessageIds.add(item.messageId);
        pendingMessages.push(item);
        console.log('[NEXUS-BIZON] Message queued', { messageId: item.messageId, userId: item.userId });
        runUserQueue(userKey);
    }

    async function runUserQueue(userKey) {
        if (!userKey || runningUsers.has(userKey)) return;
        runningUsers.add(userKey);
        while (true) {
            const index = pendingMessages.findIndex(candidate => String((candidate && candidate.userId) || '') === userKey);
            if (index < 0) break;
            const [item] = pendingMessages.splice(index, 1);
            if (!item || !item.messageId) continue;
            queuedMessageIds.delete(item.messageId);
            if (answeredMessageIds.has(item.messageId) || processingMessageIds.has(item.messageId)) continue;
            if (!isLatestMessageForUser(item)) {
                answeredMessageIds.add(item.messageId);
                continue;
            }
            processingMessageIds.add(item.messageId);
            try {
                const result = await sendToApi(item.message, item.userId, item.userName);
                const answer = String((result && result.message) || '').trim();
                if (answer) {
                    const delayMs = calculateReplyDelayMs(answer);
                    console.log('[NEXUS-BIZON] Reply delay', { messageId: item.messageId, delayMs });
                    await sleep(delayMs);
                    if (isLatestMessageForUser(item)) await doSendReply(item.messageId, item.userId, answer);
                }
                answeredMessageIds.add(item.messageId);
                messageRetryCount.delete(item.messageId);
            } catch (error) {
                const count = (messageRetryCount.get(item.messageId) || 0) + 1;
                if (count < MAX_RETRY_ATTEMPTS && isLatestMessageForUser(item)) {
                    console.error('[NEXUS-BIZON] API error, retrying:', error);
                    messageRetryCount.set(item.messageId, count);
                    processingMessageIds.delete(item.messageId);
                    await sleep(RETRY_DELAY_MS);
                    if (!queuedMessageIds.has(item.messageId) && !answeredMessageIds.has(item.messageId)) {
                        queuedMessageIds.add(item.messageId);
                        pendingMessages.push(item);
                    }
                    continue;
                }
                console.error('[NEXUS-BIZON] API error, message dropped:', error);
                answeredMessageIds.add(item.messageId);
            } finally {
                processingMessageIds.delete(item.messageId);
            }
        }
        runningUsers.delete(userKey);
        if (pendingMessages.some(candidate => String((candidate && candidate.userId) || '') === userKey)) runUserQueue(userKey);
    }

    function initObserver() {
        const chatframe = document.querySelector('#chatframe') || document.querySelector('ul#chat');
        if (!chatframe) {
            setTimeout(initObserver, 3000);
            return;
        }
        annotateAllKnownUsers(chatframe);
        refreshModerationPlatformPanel(true);
        chatframe.querySelectorAll('.msg.guest.msg_can_reply').forEach(msg => {
            const id = msg.getAttribute('data-msgid');
            if (id) answeredMessageIds.add(id);
        });
        const observer = new MutationObserver(mutations => {
            for (const mutation of mutations) {
                if (mutation.type !== 'childList') continue;
                for (const node of mutation.addedNodes) {
                    if (node.nodeType !== 1) continue;
                    const msgNodes = [];
                    if (node.matches && node.matches('.msg.guest.msg_can_reply')) msgNodes.push(node);
                    if (node.querySelectorAll) msgNodes.push(...node.querySelectorAll('.msg.guest.msg_can_reply'));
                    annotateAllKnownUsers(node);
                    refreshModerationPlatformPanel();
                    for (const msgNode of msgNodes) {
                        const messageId = msgNode.getAttribute('data-msgid');
                        if (!messageId || answeredMessageIds.has(messageId) || processingMessageIds.has(messageId)) continue;
                        processOneMessage(msgNode, messageId);
                    }
                }
            }
        });
        observer.observe(chatframe, { childList: true, subtree: true });
    }

    function processOneMessage(messageElement, messageId) {
        const msgbody = messageElement.querySelector('span.msgbody')?.textContent || '';
        const userName = messageElement.querySelector('.user')?.textContent || 'Гость';
        const userId = messageElement.getAttribute('data-userid');
        if (!msgbody || !userId) return;
        annotateKnownUser(messageElement, userId);
        if (isNoiseMessage(msgbody, userName)) {
            answeredMessageIds.add(messageId);
            return;
        }
        enqueueMessage({ message: msgbody, userId, userName, messageId, receivedAt: Date.now() });
    }

    function syncThreadMapping(bizonUserId, clientId, promptFile, threadId) {
        if (!bizonUserId || !clientId || !promptFile || !threadId) return Promise.resolve();
        return postJSON(`${apiBase()}/process2`, {
            userId: bizonUserId,
            clientId,
            assistant_id: promptFile,
            thread_id: threadId,
            room_key: getRoomKey()
        }).catch(() => null);
    }

    function sendToApi(message, userId, userName) {
        return resolveUserMapping(userId, message).then(mapping => {
            if (!mapping || !mapping.client_id) {
                throw new Error(`UTM mapping unavailable for Bizon user ${userId}; reply blocked`);
            }
            const finalUserId = mapping.client_id;
            const finalThreadId = mapping.thread_id || null;
            const promptFile = getPromptFile();
            const body = {
                user_id: finalUserId,
                thread_id: finalThreadId,
                prompt_file: promptFile,
                context: toNumber(CFG.CONTEXT, 4),
                message,
                client_name: String(userName || '').trim() || null,
                room_key: getRoomKey(),
                sec_key: CFG.SEC_KEY
            };
            return postJSON(`${apiBase()}/chat`, body).then(data => {
                const returnedThreadId = String((data && (data.thread_id || data.conversation_id)) || '').trim();
                const mappedThreadId = String((mapping && mapping.thread_id) || '').trim();
                if (returnedThreadId && returnedThreadId !== mappedThreadId) {
                    syncThreadMapping(userId, finalUserId, promptFile, returnedThreadId)
                        .then(() => console.log('[NEXUS-BIZON] Mapping synced', { userId, threadId: returnedThreadId }))
                        .catch(error => console.error('[NEXUS-BIZON] Mapping sync failed:', error));
                }
                return { message: data && data.message ? String(data.message) : '', thread_id: returnedThreadId };
            });
        });
    }

    function doSendReply(msgId, expectedUserId, answer) {
        return new Promise((resolve, reject) => {
            try {
                const msgEl = document.querySelector(`[data-msgid='${msgId}']`);
                const input = document.getElementById('inputmsg');
                const sendBtn = document.querySelector('.input-group-addon.btn-addon .btn-primary');
                if (!input || !sendBtn) return reject(new Error('Bizon UI input/send button not found'));
                if (!msgEl) return reject(new Error(`Source message ${msgId} not found; private reply cancelled`));
                if (String(msgEl.getAttribute('data-userid') || '') !== String(expectedUserId || '')) {
                    return reject(new Error(`Source message ${msgId} user mismatch; private reply cancelled`));
                }

                if (CFG.INDIVIDUAL_CHAT) {
                    const replyBtn = msgEl.querySelector('.msg_ctrls.fa.fa-comment-o') || msgEl.querySelector('.msg_ctrls.fa.fa-reply');
                    if (replyBtn) {
                        replyBtn.click();
                        setTimeout(() => {
                            if (!document.contains(msgEl) || String(msgEl.getAttribute('data-userid') || '') !== String(expectedUserId || '')) {
                                reject(new Error(`Private reply target changed for ${msgId}; send cancelled`));
                                return;
                            }
                            input.value = answer;
                            try { input.dispatchEvent(new Event('input', { bubbles: true })); } catch (_) {}
                            sendBtn.click();
                            console.log('[NEXUS-BIZON] Reply sent', { messageId: msgId });
                            resolve();
                        }, 400);
                        return;
                    }
                    return reject(new Error(`Private reply control missing for ${msgId}; public fallback disabled`));
                }
                input.value = answer;
                try { input.dispatchEvent(new Event('input', { bubbles: true })); } catch (_) {}
                sendBtn.click();
                console.log('[NEXUS-BIZON] Reply sent', { messageId: msgId });
                resolve();
            } catch (error) {
                reject(error);
            }
        });
    }

    setTimeout(initObserver, 3000);
    setInterval(() => refreshModerationPlatformPanel(), 5000);
})();
