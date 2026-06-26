(function() {
    'use strict';

    if (window.__nexusBizonRoomLoaderInstalled) return;
    window.__nexusBizonRoomLoaderInstalled = true;

    const parts = window.location.pathname.split('/').filter(Boolean);
    const room = decodeURIComponent(parts[parts.length - 1] || '').trim();
    const configUrl = `https://junior.sobakovod.pro/nexus/bizon/api/public/room-config?room=${encodeURIComponent(room)}`;

    fetch(configUrl, { method: 'GET', mode: 'cors', credentials: 'omit' })
        .then(async response => {
            let data = null;
            try { data = await response.json(); } catch (_) {}
            if (!response.ok || !data || !data.config || !data.script_src) {
                throw new Error((data && (data.detail || data.error)) || `HTTP ${response.status}`);
            }
            window.BOT_CONFIG = Object.assign({}, window.BOT_CONFIG || {}, data.config);
            const script = document.createElement('script');
            script.defer = true;
            script.src = data.script_src;
            script.onerror = () => console.error('[NEXUS-BIZON] Runtime load failed', script.src);
            document.head.appendChild(script);
        })
        .catch(error => console.error('[NEXUS-BIZON] Room config load failed', { room, error }));
})();
