const socket = io();

let myPeerSid = null;
let myFriendlyName = 'Initializing...';
let isUserLoggedIn = false;

socket.on('connect', () => {
    myPeerSid = socket.id;
    logEvent(`Connected to signaling server tracker (Socket ID: ${myPeerSid.substring(0, 8)}...)`);
    
    fetch('/api/user_info')
        .then(res => res.json())
        .then(data => {
            if (data.logged_in) {
                isUserLoggedIn = true;
                myFriendlyName = data.node_id;
                updateHeaderNodeStatus(`Connected as ${data.node_id} (${data.username})`, 'online');
            } else {
                isUserLoggedIn = false;
                myFriendlyName = `Node-${myPeerSid.substring(0, 4).toUpperCase()}`;
                updateHeaderNodeStatus(`Connected as ${myFriendlyName} (Guest)`, 'online');
            }

            if (typeof loadStoredFilesFromIDB === 'function') {
                loadStoredFilesFromIDB();
            }
        });

    requestSwarmCatalog();
});

socket.on('disconnect', () => {
    updateHeaderNodeStatus('Disconnected from Tracker', 'offline');
    logEvent('Disconnected from signaling server.');
});

socket.on('assigned_peer_name', (data) => {
    myFriendlyName = data.friendly_name;
    updateHeaderNodeStatus(`Connected as ${data.friendly_name}`, 'online');
});

socket.on('catalog_update', (data) => {
    renderSwarmCatalogUI(data.catalog);
});

socket.on('file_registered_ack', (data) => {
    logEvent(`File "${data.filename}" registered on tracker catalog.`);
    requestSwarmCatalog();
});

socket.on('file_unregistered_ack', (data) => {
    logEvent(`File hash ${data.file_hash.substring(0, 10)}... removed from tracker.`);
    requestSwarmCatalog();
});

socket.on('swarm_info', (data) => {
    logEvent(`Swarm metadata received for hash: ${data.file_hash.substring(0, 10)}...`);
    window.initiateSwarmConnections(data.peers, data.file_info.total_chunks, data.file_info.filename);
});

socket.on('connect_to_leecher', (data) => {
    logEvent(`Initiating connection offer to newly joined leecher: ${data.friendly_name}`);
    if (typeof createPeerConnection === 'function') {
        createPeerConnection(data.leecher_sid, true);
    }
});

socket.on('peer_left', (data) => {
    logEvent(`Peer left swarm: ${data.friendly_name}`);
    removePeerFromUI(data.sid);
});

socket.on('signal_offer', (data) => {
    if (typeof handleIncomingOffer === 'function') {
        handleIncomingOffer(data.from, data.offer);
    }
});

socket.on('signal_answer', (data) => {
    if (typeof handleIncomingAnswer === 'function') {
        handleIncomingAnswer(data.from, data.answer);
    }
});

socket.on('signal_ice', (data) => {
    if (typeof handleIncomingICE === 'function') {
        handleIncomingICE(data.from, data.candidate);
    }
});

function requestSwarmCatalog() {
    socket.emit('get_swarm_catalog');
}

function renderSwarmCatalogUI(catalog) {
    const tbody = document.getElementById('catalog-list');
    if (!tbody) return;

    if (!catalog || catalog.length === 0) {
        tbody.innerHTML = '<tr><td colspan="6" class="empty-msg">No active files in swarm catalog. Upload one to begin!</td></tr>';
        return;
    }

    tbody.innerHTML = '';
    catalog.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><strong>${item.filename}</strong></td>
            <td><code>${item.file_hash.substring(0, 10)}...</code></td>
            <td>${item.total_chunks}</td>
            <td><span style="color: #4ade80; font-weight: bold;">${item.seeders}</span></td>
            <td><span style="color: #f87171; font-weight: bold;">${item.leechers}</span></td>
            <td>
                <button class="btn-small" onclick="quickDownloadFromCatalog('${item.file_hash}')">Download</button>
            </td>
        `;
        tbody.appendChild(tr);
    });
}

function quickDownloadFromCatalog(hash) {
    document.getElementById('hash-input').value = hash;
    joinSwarmAndDownload();
}

function filterSwarmCatalog() {
    const query = document.getElementById('search-input').value.toLowerCase();
    const rows = document.querySelectorAll('#catalog-list tr');
    rows.forEach(row => {
        const filenameCell = row.cells[0];
        if (filenameCell) {
            const text = filenameCell.textContent.toLowerCase();
            row.style.display = text.includes(query) ? '' : 'none';
        }
    });
}

function updateHeaderNodeStatus(text, stateClass) {
    const elem = document.getElementById('node-status');
    if (elem) {
        elem.textContent = text;
        elem.className = `status-${stateClass}`;
    }
}

function logEvent(msg) {
    const logWin = document.getElementById('log-window');
    if (logWin) {
        const time = new Date().toLocaleTimeString();
        const div = document.createElement('div');
        div.textContent = `[${time}] ${msg}`;
        logWin.appendChild(div);
        logWin.scrollTop = logWin.scrollHeight;
    }
}

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    if (!container) return;
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 4000);
}

function removePeerFromUI(sid) {
    const elem = document.getElementById(`peer-item-${sid}`);
    if (elem) elem.remove();
}