// Initialize Socket.IO connection
const socket = io("https://p2p-swarm-app.onrender.com/");

// UI Elements
const nodeStatus = document.getElementById('node-status');
const logWindow = document.getElementById('log-window');
const peerListUI = document.getElementById('peer-list');

// Log helper function
function logEvent(message) {
    const time = new Date().toLocaleTimeString();
    const logItem = document.createElement('div');
    logItem.textContent = `[${time}] ${message}`;
    logWindow.appendChild(logItem);
    logWindow.scrollTop = logWindow.scrollHeight;
}

// Socket Connection Events
socket.on('connect', () => {
    nodeStatus.textContent = `Connected to Tracker (ID: ${socket.id})`;
    nodeStatus.className = 'status-online';
    logEvent('Successfully connected to signaling server tracker.');
});

socket.on('disconnect', () => {
    nodeStatus.textContent = 'Disconnected from Tracker';
    nodeStatus.className = 'status-offline';
    logEvent('Lost connection to signaling server.');
});

// Tracker Event Listeners
socket.on('file_registered_ack', (data) => {
    logEvent(`File successfully registered on tracker with Hash: ${data.file_hash.substring(0, 10)}...`);
});

socket.on('swarm_info', (data) => {
    logEvent(`Joined swarm for file: "${data.file_info.filename}". Chunks: ${data.file_info.total_chunks}`);
    updatePeerListUI(data.peers);
    
    // Trigger WebRTC connections to all active seeders/peers
    window.initiateSwarmConnections(data.peers, data.file_info.total_chunks, data.file_info.filename);
});

socket.on('peer_joined', (data) => {
    logEvent(`New peer joined swarm: ${data.sid} (${data.type})`);
    addPeerToUI(data.sid, data.type);
});

socket.on('peer_left', (data) => {
    logEvent(`Peer left swarm: ${data.sid}`);
    removePeerFromUI(data.sid);
});

socket.on('swarm_error', (data) => {
    logEvent(`Swarm Error: ${data.message}`);
    alert(data.message);
});

// WebRTC Signaling Event Listeners
socket.on('signal_offer', (data) => {
    window.handleIncomingOffer(data.from, data.offer);
});

socket.on('signal_answer', (data) => {
    window.handleIncomingAnswer(data.from, data.answer);
});

socket.on('signal_ice', (data) => {
    window.handleIncomingICE(data.from, data.candidate);
});

// Telemetry UI Helpers
function updatePeerListUI(peers) {
    peerListUI.innerHTML = '';
    if (peers.length === 0) {
        peerListUI.innerHTML = '<li class="empty-msg">No active swarm connections.</li>';
        return;
    }
    peers.forEach(p => addPeerToUI(p.sid, p.type));
}

function addPeerToUI(sid, type) {
    const emptyMsg = peerListUI.querySelector('.empty-msg');
    if (emptyMsg) emptyMsg.remove();

    if (!document.getElementById(`peer-${sid}`)) {
        const li = document.createElement('li');
        li.id = `peer-${sid}`;
        li.className = 'peer-item';
        li.innerHTML = `
            <div style="display:flex; justify-between; align-items:center;">
                <strong>Peer:</strong> ${sid.substring(0, 8)}... <span style="color:#38bdf8;" id="role-${sid}">[${type}]</span>
            </div>
            <div style="margin-top: 4px; font-size: 0.8rem; color: #a7f3d0;">
                Chunks Provided: <span id="chunk-count-${sid}">0</span> (<span id="chunk-pct-${sid}">0%</span>)
            </div>
        `;
        peerListUI.appendChild(li);
    }
}

function removePeerFromUI(sid) {
    const peerElem = document.getElementById(`peer-${sid}`);
    if (peerElem) peerElem.remove();
    if (peerListUI.children.length === 0) {
        peerListUI.innerHTML = '<li class="empty-msg">No active swarm connections.</li>';
    }
}