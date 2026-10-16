const socket = io("https://p2p-swarm-app.onrender.com/");

let myFriendlyName = "Connecting...";
let activeSwarmCatalog = [];

// UI Elements
const nodeStatus = document.getElementById('node-status');
const logWindow = document.getElementById('log-window');
const peerListUI = document.getElementById('peer-list');
const activeLeechersCountElem = document.getElementById('active-leechers-count');

function showToast(message, type = 'info') {
    const container = document.getElementById('toast-container');
    const toast = document.createElement('div');
    toast.className = `toast toast-${type}`;
    toast.textContent = message;
    container.appendChild(toast);
    setTimeout(() => toast.remove(), 3500);
}

function logEvent(message) {
    const time = new Date().toLocaleTimeString();
    const logItem = document.createElement('div');
    logItem.textContent = `[${time}] ${message}`;
    logWindow.appendChild(logItem);
    logWindow.scrollTop = logWindow.scrollHeight;
}

// Socket Events
socket.on('connect', () => {
    logEvent('Successfully connected to signaling server tracker.');
    showToast('Connected to Central Tracker!', 'success');
    socket.emit('get_swarm_catalog');
    if (window.reRegisterStoredFiles) {
        window.reRegisterStoredFiles();
    }
});

socket.on('assigned_peer_name', (data) => {
    myFriendlyName = data.friendly_name;
    nodeStatus.textContent = `Connected as ${myFriendlyName}`;
    nodeStatus.className = 'status-online';
});

socket.on('disconnect', () => {
    nodeStatus.textContent = 'Disconnected from Tracker';
    nodeStatus.className = 'status-offline';
    logEvent('Lost connection to signaling server.');
    showToast('Disconnected from Tracker', 'error');
});

document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
        if (!socket.connected) {
            logEvent('App re-focused. Reconnecting to tracker...');
            socket.connect();
        } else {
            socket.emit('get_swarm_catalog');
        }
    }
});

// Swarm Catalog
socket.on('catalog_update', (data) => {
    activeSwarmCatalog = data.catalog;
    renderSwarmCatalog(activeSwarmCatalog);
});

function requestSwarmCatalog() {
    showToast('Refreshing catalog...', 'info');
    socket.emit('get_swarm_catalog');
}

function renderSwarmCatalog(catalogList) {
    const catalogTbody = document.getElementById('catalog-list');
    catalogTbody.innerHTML = '';
    
    if (catalogList.length === 0) {
        catalogTbody.innerHTML = '<tr><td colspan="6" class="empty-msg">No active files in swarm catalog. Upload one to begin!</td></tr>';
        return;
    }
    
    catalogList.forEach(item => {
        const tr = document.createElement('tr');
        tr.innerHTML = `
            <td><strong>${item.filename}</strong></td>
            <td><code>${item.file_hash.substring(0, 10)}...</code></td>
            <td>${item.total_chunks} (${(item.total_chunks * 16 / 1024).toFixed(1)} MB)</td>
            <td style="color:#4ade80; font-weight:bold;">${item.seeders}</td>
            <td style="color:#f87171; font-weight:bold;">${item.leechers}</td>
            <td>
                <button class="btn-small" onclick="selectFileFromCatalog('${item.file_hash}', '${item.filename}', ${item.total_chunks})">Download</button>
            </td>
        `;
        catalogTbody.appendChild(tr);
    });
}

function filterSwarmCatalog() {
    const query = document.getElementById('search-input').value.toLowerCase().trim();
    const filtered = activeSwarmCatalog.filter(f => f.filename.toLowerCase().includes(query) || f.file_hash.includes(query));
    renderSwarmCatalog(filtered);
}

function selectFileFromCatalog(fileHash, filename, totalChunks) {
    document.getElementById('hash-input').value = fileHash;
    showToast(`Joining swarm for ${filename}...`, 'info');
    joinSwarmAndDownload();
}

// Tracker Event Listeners
socket.on('file_registered_ack', (data) => {
    logEvent(`File "${data.filename}" registered on tracker.`);
    showToast(`File "${data.filename}" is now seeding!`, 'success');
    window.addFileToSeededListUI(data.file_hash, data.filename, data.total_chunks);
});

socket.on('swarm_info', (data) => {
    logEvent(`Joined swarm for file: "${data.file_info.filename}". Chunks: ${data.file_info.total_chunks}`);
    updatePeerListUI(data.peers);
    window.initiateSwarmConnections(data.peers, data.file_info.total_chunks, data.file_info.filename);
});

socket.on('peer_joined', (data) => {
    logEvent(`New peer joined swarm: ${data.friendly_name} (${data.type})`);
    addPeerToUI(data.sid, data.friendly_name, data.type);
});

socket.on('peer_left', (data) => {
    logEvent(`Peer left swarm: ${data.friendly_name}`);
    removePeerFromUI(data.sid);
});

socket.on('swarm_error', (data) => {
    logEvent(`Swarm Error: ${data.message}`);
    showToast(data.message, 'error');
});

// WebRTC Signaling
socket.on('signal_offer', (data) => window.handleIncomingOffer(data.from, data.offer));
socket.on('signal_answer', (data) => window.handleIncomingAnswer(data.from, data.answer));
socket.on('signal_ice', (data) => window.handleIncomingICE(data.from, data.candidate));

function updatePeerListUI(peers) {
    peerListUI.innerHTML = '';
    if (peers.length === 0) {
        peerListUI.innerHTML = '<li class="empty-msg">No active swarm connections.</li>';
        activeLeechersCountElem.textContent = '0';
        return;
    }
    peers.forEach(p => addPeerToUI(p.sid, p.friendly_name, p.type));
}

function addPeerToUI(sid, friendlyName, type) {
    const emptyMsg = peerListUI.querySelector('.empty-msg');
    if (emptyMsg) emptyMsg.remove();

    if (!document.getElementById(`peer-${sid}`)) {
        const li = document.createElement('li');
        li.id = `peer-${sid}`;
        li.className = 'peer-item';
        li.innerHTML = `
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <strong>Peer:</strong> ${friendlyName} <span style="color:#38bdf8;" id="role-${sid}">[${type}]</span>
            </div>
            <div style="margin-top: 4px; font-size: 0.8rem; color: #a7f3d0;">
                Chunks Serviced: <span id="chunk-count-${sid}">0</span> (<span id="chunk-pct-${sid}">0%</span>)
            </div>
        `;
        peerListUI.appendChild(li);
    }
    updateActiveLeechersCount();
}

function removePeerFromUI(sid) {
    const peerElem = document.getElementById(`peer-${sid}`);
    if (peerElem) peerElem.remove();
    if (peerListUI.children.length === 0) {
        peerListUI.innerHTML = '<li class="empty-msg">No active swarm connections.</li>';
    }
    updateActiveLeechersCount();
}

function updateActiveLeechersCount() {
    const peerItems = peerListUI.querySelectorAll('.peer-item');
    activeLeechersCountElem.textContent = peerItems.length.toString();
}

window.addEventListener('DOMContentLoaded', () => {
    const urlParams = new URLSearchParams(window.location.hash.substring(1));
    const fileHash = urlParams.get('fileHash');
    if (fileHash) {
        document.getElementById('hash-input').value = fileHash;
        setTimeout(() => joinSwarmAndDownload(), 800);
    }
});