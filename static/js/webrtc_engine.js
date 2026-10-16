const rtcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        {
            urls: 'turn:openrelay.metered.ca:80',
            username: 'openrelay',
            credential: 'openrelay'
        },
        {
            urls: 'turn:openrelay.metered.ca:443',
            username: 'openrelay',
            credential: 'openrelay'
        }
    ]
};

const CHUNK_SIZE = 16384; 

let multiSeederStore = {};
let fileMetaStore = {}; 
let mySeededFilesMeta = [];

let receivedChunks = {};   
let peerConnections = {}; 
let dataChannels = {};    
let iceCandidatesQueue = {}; 
let targetTotalChunks = 0;
let targetFilename = '';
let currentFileHash = '';
let chunkRetryInterval = null;

// Telemetry sets per peer
let peerServedChunkSets = {};
let peerReceivedChunkSets = {};

// --- INDEXEDDB STORAGE SYSTEM ---
let db;
const dbRequest = indexedDB.open("SwarmP2PStorage", 1);

dbRequest.onupgradeneeded = function(e) {
    db = e.target.result;
    if (!db.objectStoreNames.contains("seeded_files")) {
        db.createObjectStore("seeded_files", { keyPath: "fileHash" });
    }
};

dbRequest.onsuccess = function(e) {
    db = e.target.result;
    loadStoredFilesFromIDB();
};

function saveFileToIDB(fileHash, filename, chunks) {
    if (!db) return;
    const tx = db.transaction("seeded_files", "readwrite");
    const store = tx.objectStore("seeded_files");
    store.put({ fileHash, filename, chunks });
}

function loadStoredFilesFromIDB() {
    if (!db) return;
    const tx = db.transaction("seeded_files", "readonly");
    const store = tx.objectStore("seeded_files");
    const req = store.getAll();

    req.onsuccess = function() {
        const records = req.result || [];
        records.forEach(rec => {
            multiSeederStore[rec.fileHash] = rec.chunks;
            fileMetaStore[rec.fileHash] = rec.chunks.length;
            window.addFileToSeededListUI(rec.fileHash, rec.filename, rec.chunks.length);
        });
        window.reRegisterStoredFiles();
    };
}

window.reRegisterStoredFiles = function() {
    if (Object.keys(multiSeederStore).length > 0 && socket && socket.connected) {
        mySeededFilesMeta.forEach(item => {
            socket.emit('register_file', {
                file_hash: item.hash,
                filename: item.name,
                total_chunks: item.chunks
            });
        });
    }
};

function clearLocalStorageAndReload() {
    if (confirm("Are you sure you want to clear locally saved files on this node?")) {
        const req = indexedDB.deleteDatabase("SwarmP2PStorage");
        req.onsuccess = function() {
            showToast("Local P2P storage cleared!", "success");
            setTimeout(() => location.reload(), 500);
        };
    }
}

// --- SEEDER LOGIC: Atomic File Processing Before Catalog Registration ---
async function registerAndSeedFile() {
    const fileInput = document.getElementById('file-input');
    const seedBtn = document.getElementById('seed-btn');
    
    if (!fileInput.files.length) {
        showToast('Please select a file to seed.', 'error');
        return;
    }
    
    const file = fileInput.files[0];
    seedBtn.disabled = true;
    seedBtn.textContent = "Processing File (0%)...";
    logEvent(`Reading file "${file.name}" (${(file.size / (1024 * 1024)).toFixed(2)} MB)...`);
    
    let fileChunks = [];
    let offset = 0;
    const totalChunks = Math.ceil(file.size / CHUNK_SIZE);
    
    while (offset < file.size) {
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const buffer = await slice.arrayBuffer();
        fileChunks.push(buffer);
        offset += CHUNK_SIZE;
        
        const readPercent = Math.floor((fileChunks.length / totalChunks) * 100);
        seedBtn.textContent = `Processing File (${readPercent}%)...`;
    }
    
    const fileHash = await computeSimpleHash(`${file.name}-${file.size}-${file.lastModified}`);
    
    multiSeederStore[fileHash] = fileChunks;
    fileMetaStore[fileHash] = fileChunks.length;
    currentFileHash = fileHash;

    saveFileToIDB(fileHash, file.name, fileChunks);

    socket.emit('register_file', {
        file_hash: fileHash,
        filename: file.name,
        total_chunks: fileChunks.length
    });

    fileInput.value = "";
    seedBtn.disabled = false;
    seedBtn.textContent = "Seed File to Swarm";
    showToast(`File ready! Registered on Swarm Catalog.`, 'success');
}

window.addFileToSeededListUI = function(fileHash, filename, totalChunks) {
    if (!mySeededFilesMeta.some(f => f.hash === fileHash)) {
        mySeededFilesMeta.push({ hash: fileHash, name: filename, chunks: totalChunks });
    }
    
    const tbody = document.getElementById('seeded-files-list');
    tbody.innerHTML = '';
    
    mySeededFilesMeta.forEach(f => {
        const tr = document.createElement('tr');
        const shareableLink = `${window.location.origin}/#fileHash=${f.hash}`;
        tr.innerHTML = `
            <td><strong>${f.name}</strong></td>
            <td>${f.chunks}</td>
            <td>
                <button class="btn-small" onclick="copyMagnetLink('${shareableLink}')">Copy Link</button>
            </td>
        `;
        tbody.appendChild(tr);
    });
};

function copyMagnetLink(link) {
    navigator.clipboard.writeText(link);
    showToast('Magnet link copied to clipboard!', 'success');
}

// --- LEECHER LOGIC ---
function joinSwarmAndDownload() {
    const hashInput = document.getElementById('hash-input').value.trim();
    const joinBtn = document.getElementById('join-btn');
    
    let fileHash = hashInput;
    if (hashInput.includes('fileHash=')) {
        fileHash = hashInput.split('fileHash=')[1].split('&')[0];
    }

    if (!fileHash) {
        showToast('Please enter a valid File Hash or Link.', 'error');
        return;
    }

    if (joinBtn.disabled) return;
    
    currentFileHash = fileHash;
    receivedChunks = {};
    peerServedChunkSets = {};
    peerReceivedChunkSets = {};
    
    joinBtn.disabled = true;
    joinBtn.textContent = "Connecting to Swarm...";

    document.getElementById('download-progress-box').classList.remove('hidden');
    document.getElementById('progress-fill').style.width = '0%';
    document.getElementById('dl-progress-text').textContent = '0%';
    logEvent(`Requesting swarm telemetry for Hash: ${currentFileHash.substring(0, 10)}...`);
    
    socket.emit('join_swarm', { file_hash: currentFileHash });
}

window.initiateSwarmConnections = function(peers, totalChunks, filename) {
    const joinBtn = document.getElementById('join-btn');
    joinBtn.disabled = false;
    joinBtn.textContent = "Join Swarm & Download";

    targetTotalChunks = totalChunks;
    targetFilename = filename;
    fileMetaStore[currentFileHash] = totalChunks;

    document.getElementById('dl-filename').textContent = filename;
    document.getElementById('dl-chunks-text').textContent = `0/${totalChunks}`;
    
    if (peers.length === 0) {
        logEvent('Warning: No seeders currently available in swarm.');
        showToast('No active seeders found in swarm.', 'error');
        return;
    }
    
    showToast(`Connecting to ${peers.length} active seeder(s)...`, 'info');
    peers.forEach(peer => createPeerConnection(peer.sid, true));

    if (chunkRetryInterval) clearInterval(chunkRetryInterval);
    chunkRetryInterval = setInterval(() => {
        if (Object.keys(receivedChunks).length < targetTotalChunks) {
            requestMissingChunks();
        } else {
            clearInterval(chunkRetryInterval);
        }
    }, 500);
};

// --- WEBRTC PIPELINE ---
function createPeerConnection(targetSid, isInitiator) {
    if (peerConnections[targetSid]) {
        const existingState = peerConnections[targetSid].connectionState;
        if (existingState === 'connected' || existingState === 'connecting') {
            return peerConnections[targetSid];
        }
    }

    const pc = new RTCPeerConnection(rtcConfig);
    peerConnections[targetSid] = pc;
    iceCandidatesQueue[targetSid] = [];

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            socket.emit('signal_ice', { target: targetSid, candidate: event.candidate });
        }
    };

    pc.onconnectionstatechange = () => {
        logEvent(`Peer Connection State with ${targetSid.substring(0, 8)}: ${pc.connectionState}`);
    };

    if (isInitiator) {
        const dc = pc.createDataChannel('fileTransferChannel');
        setupDataChannelEvents(dc, targetSid);
        dataChannels[targetSid] = dc;

        pc.createOffer()
            .then(offer => pc.setLocalDescription(offer))
            .then(() => socket.emit('signal_offer', { target: targetSid, offer: pc.localDescription }))
            .catch(err => logEvent(`Error creating offer: ${err}`));
    } else {
        pc.ondatachannel = (event) => {
            logEvent(`Incoming DataChannel received from Peer: ${targetSid.substring(0, 8)}`);
            const dc = event.channel;
            setupDataChannelEvents(dc, targetSid);
            dataChannels[targetSid] = dc;
        };
    }

    return pc;
}

function setupDataChannelEvents(dc, targetSid) {
    dc.binaryType = 'arraybuffer';
    
    dc.onopen = () => {
        logEvent(`P2P DataChannel OPENED with Peer: ${targetSid.substring(0, 8)}...`);
        showToast('P2P Direct Channel Opened!', 'success');
        requestMissingChunks();
    };

    dc.onmessage = (event) => {
        if (typeof event.data === 'string') {
            const msg = JSON.parse(event.data);
            if (msg.type === 'request_chunk') {
                sendChunkToPeer(targetSid, msg.chunkIndex, msg.fileHash);
                updateSeederProgressUI(targetSid, msg.fileHash, msg.chunkIndex);
            }
        } else {
            handleIncomingChunkBuffer(event.data, targetSid);
        }
    };

    dc.onclose = () => logEvent(`DataChannel CLOSED with Peer: ${targetSid.substring(0, 8)}...`);
    dc.onerror = (err) => logEvent(`DataChannel Error: ${err}`);
}

function updateSeederProgressUI(targetSid, fileHash, chunkIndex) {
    if (!peerServedChunkSets[targetSid]) {
        peerServedChunkSets[targetSid] = new Set();
    }
    
    peerServedChunkSets[targetSid].add(chunkIndex);
    
    const uniqueCount = peerServedChunkSets[targetSid].size;
    const total = fileMetaStore[fileHash] || targetTotalChunks || 1;
    const percentage = Math.min(100, ((uniqueCount / total) * 100)).toFixed(1);

    const countElem = document.getElementById(`chunk-count-${targetSid}`);
    const pctElem = document.getElementById(`chunk-pct-${targetSid}`);
    if (countElem && pctElem) {
        countElem.textContent = uniqueCount;
        pctElem.textContent = `${percentage}%`;
    }
}

// --- SIGNALING HANDLERS ---
window.handleIncomingOffer = async function(fromSid, offer) {
    const pc = createPeerConnection(fromSid, false);
    try {
        await pc.setRemoteDescription(new RTCSessionDescription(offer));
        if (iceCandidatesQueue[fromSid]) {
            while (iceCandidatesQueue[fromSid].length > 0) {
                const cand = iceCandidatesQueue[fromSid].shift();
                await pc.addIceCandidate(new RTCIceCandidate(cand));
            }
        }
        const answer = await pc.createAnswer();
        await pc.setLocalDescription(answer);
        socket.emit('signal_answer', { target: fromSid, answer: pc.localDescription });
    } catch (e) {
        logEvent(`Error handling offer: ${e}`);
    }
};

window.handleIncomingAnswer = async function(fromSid, answer) {
    const pc = peerConnections[fromSid];
    if (pc) {
        try {
            await pc.setRemoteDescription(new RTCSessionDescription(answer));
            if (iceCandidatesQueue[fromSid]) {
                while (iceCandidatesQueue[fromSid].length > 0) {
                    const cand = iceCandidatesQueue[fromSid].shift();
                    await pc.addIceCandidate(new RTCIceCandidate(cand));
                }
            }
        } catch (e) {
            logEvent(`Error setting answer: ${e}`);
        }
    }
};

window.handleIncomingICE = async function(fromSid, candidate) {
    const pc = peerConnections[fromSid];
    if (pc && pc.remoteDescription && pc.remoteDescription.type) {
        try {
            await pc.addIceCandidate(new RTCIceCandidate(candidate));
        } catch (e) {
            logEvent(`Error adding ICE: ${e}`);
        }
    } else {
        if (!iceCandidatesQueue[fromSid]) iceCandidatesQueue[fromSid] = [];
        iceCandidatesQueue[fromSid].push(candidate);
    }
};

// --- CHUNK TRANSFER WITH TRUE PARALLEL SWARM BALANCING ---
function requestMissingChunks() {
    const openPeerSids = Object.keys(dataChannels).filter(
        sid => dataChannels[sid] && dataChannels[sid].readyState === 'open'
    );
    
    if (openPeerSids.length === 0) return;

    const missingChunkIndices = [];
    for (let i = 0; i < targetTotalChunks; i++) {
        if (!receivedChunks.hasOwnProperty(i)) {
            missingChunkIndices.push(i);
        }
    }

    if (missingChunkIndices.length === 0) return;

    missingChunkIndices.forEach((chunkIndex, idx) => {
        const targetSid = openPeerSids[idx % openPeerSids.length];
        const dc = dataChannels[targetSid];

        if (dc && dc.readyState === 'open' && dc.bufferedAmount < 65536) {
            dc.send(JSON.stringify({
                type: 'request_chunk',
                chunkIndex: chunkIndex,
                fileHash: currentFileHash
            }));
        }
    });
}

function sendChunkToPeer(targetSid, chunkIndex, requestedHash) {
    const dc = dataChannels[targetSid];
    const sourceChunks = multiSeederStore[requestedHash];
    
    if (dc && dc.readyState === 'open' && sourceChunks && sourceChunks[chunkIndex]) {
        const chunkBuffer = sourceChunks[chunkIndex];
        const header = new Int32Array([chunkIndex]);
        const packet = new Uint8Array(header.byteLength + chunkBuffer.byteLength);
        
        packet.set(new Uint8Array(header.buffer), 0);
        packet.set(new Uint8Array(chunkBuffer), header.byteLength);
        
        try {
            dc.send(packet.buffer);
        } catch (e) {
            console.error(`Error sending chunk ${chunkIndex}:`, e);
        }
    }
}

function handleIncomingChunkBuffer(buffer, senderSid) {
    const header = new Int32Array(buffer.slice(0, 4));
    const chunkIndex = header[0];
    const chunkData = buffer.slice(4);

    if (!receivedChunks.hasOwnProperty(chunkIndex)) {
        receivedChunks[chunkIndex] = chunkData;

        if (!peerReceivedChunkSets[senderSid]) {
            peerReceivedChunkSets[senderSid] = new Set();
        }
        peerReceivedChunkSets[senderSid].add(chunkIndex);

        const peerChunksReceived = peerReceivedChunkSets[senderSid].size;
        const peerPercentage = Math.min(100, ((peerChunksReceived / targetTotalChunks) * 100)).toFixed(1);
        
        const countElem = document.getElementById(`chunk-count-${senderSid}`);
        const pctElem = document.getElementById(`chunk-pct-${senderSid}`);
        if (countElem && pctElem) {
            countElem.textContent = peerChunksReceived;
            pctElem.textContent = `${peerPercentage}%`;
        }

        const downloadedCount = Object.keys(receivedChunks).length;
        const progressPercent = Math.floor((downloadedCount / targetTotalChunks) * 100);

        document.getElementById('progress-fill').style.width = `${progressPercent}%`;
        document.getElementById('dl-progress-text').textContent = `${progressPercent}%`;
        document.getElementById('dl-chunks-text').textContent = `${downloadedCount}/${targetTotalChunks}`;

        if (downloadedCount === targetTotalChunks) {
            if (chunkRetryInterval) clearInterval(chunkRetryInterval);
            logEvent(`SUCCESS: All ${targetTotalChunks} chunks retrieved via P2P Swarm! Assembling file...`);
            showToast(`File download complete! Assembling...`, 'success');
            
            const assembledChunks = Object.keys(receivedChunks).map(k => receivedChunks[k]);
            multiSeederStore[currentFileHash] = assembledChunks;
            fileMetaStore[currentFileHash] = targetTotalChunks;
            saveFileToIDB(currentFileHash, targetFilename, assembledChunks);

            socket.emit('register_file', {
                file_hash: currentFileHash,
                filename: targetFilename,
                total_chunks: targetTotalChunks
            });
            assembleAndDownloadFile();
        }
    }
}

function assembleAndDownloadFile() {
    const orderedBuffers = [];
    for (let i = 0; i < targetTotalChunks; i++) {
        orderedBuffers.push(receivedChunks[i]);
    }
    
    const blob = new Blob(orderedBuffers);
    const downloadUrl = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = targetFilename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    logEvent(`File "${targetFilename}" saved to disk & ready for seeding.`);
}

async function computeSimpleHash(input) {
    const encoder = new TextEncoder();
    const data = encoder.encode(input);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}