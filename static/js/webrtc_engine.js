// WebRTC Configuration with Multiple Reliable Public STUN Servers
const rtcConfig = {
    iceServers: [
        { urls: 'stun:stun.l.google.com:19302' },
        { urls: 'stun:stun1.l.google.com:19302' },
        { urls: 'stun:stun2.l.google.com:19302' },
        { urls: 'stun:stun3.l.google.com:19302' },
        { urls: 'stun:stun4.l.google.com:19302' }
    ]
};

// Application State
const CHUNK_SIZE = 16384; // 16 KB per chunk
let localFileChunks = [];  
let receivedChunks = {};   
let peerConnections = {}; 
let dataChannels = {};    
let targetTotalChunks = 0;
let targetFilename = '';
let currentFileHash = '';
let chunkRetryInterval = null;

// --- SEEDER LOGIC ---
async function registerAndSeedFile() {
    const fileInput = document.getElementById('file-input');
    if (!fileInput.files.length) {
        alert('Please select a file to seed.');
        return;
    }
    const file = fileInput.files[0];
    logEvent(`Processing file "${file.name}" (${file.size} bytes)...`);
    
    localFileChunks = [];
    let offset = 0;
    while (offset < file.size) {
        const slice = file.slice(offset, offset + CHUNK_SIZE);
        const buffer = await slice.arrayBuffer();
        localFileChunks.push(buffer);
        offset += CHUNK_SIZE;
    }
    
    currentFileHash = await computeSimpleHash(`${file.name}-${file.size}-${file.lastModified}`);
    
    document.getElementById('seed-file-hash').textContent = currentFileHash;
    document.getElementById('seed-chunk-count').textContent = localFileChunks.length;
    document.getElementById('seed-info').classList.remove('hidden');
    
    socket.emit('register_file', {
        file_hash: currentFileHash,
        filename: file.name,
        total_chunks: localFileChunks.length
    });
}

// --- LEECHER LOGIC ---
function joinSwarmAndDownload() {
    const hashInput = document.getElementById('hash-input').value.trim();
    if (!hashInput) {
        alert('Please enter a valid File Hash.');
        return;
    }
    currentFileHash = hashInput;
    receivedChunks = {};
    
    document.getElementById('download-progress-box').classList.remove('hidden');
    document.getElementById('progress-fill').style.width = '0%';
    document.getElementById('dl-progress-text').textContent = '0%';
    logEvent(`Requesting swarm telemetry from tracker for Hash: ${currentFileHash.substring(0, 10)}...`);
    socket.emit('join_swarm', { file_hash: currentFileHash });
}

window.initiateSwarmConnections = function(peers, totalChunks, filename) {
    targetTotalChunks = totalChunks;
    targetFilename = filename;
    document.getElementById('dl-filename').textContent = filename;
    document.getElementById('dl-chunks-text').textContent = `0/${totalChunks}`;
    if (peers.length === 0) {
        logEvent('Warning: No seeders currently available in swarm.');
        return;
    }
    
    peers.forEach(peer => {
        createPeerConnection(peer.sid, true);
    });

    // Keep checking and requesting missing chunks every 1 second until complete
    if (chunkRetryInterval) clearInterval(chunkRetryInterval);
    chunkRetryInterval = setInterval(() => {
        if (Object.keys(receivedChunks).length < targetTotalChunks) {
            requestMissingChunks();
        } else {
            clearInterval(chunkRetryInterval);
        }
    }, 1000);
};

// --- WEBRTC PIPELINE ---
function createPeerConnection(targetSid, isInitiator) {
    const pc = new RTCPeerConnection(rtcConfig);
    peerConnections[targetSid] = pc;

    pc.onicecandidate = (event) => {
        if (event.candidate) {
            socket.emit('signal_ice', { target: targetSid, candidate: event.candidate });
        }
    };

    if (isInitiator) {
        const dc = pc.createDataChannel('fileTransferChannel');
        setupDataChannelEvents(dc, targetSid);
        dataChannels[targetSid] = dc;

        pc.createOffer()
            .then(offer => pc.setLocalDescription(offer))
            .then(() => {
                socket.emit('signal_offer', { target: targetSid, offer: pc.localDescription });
            });
    } else {
        pc.ondatachannel = (event) => {
            const dc = event.channel;
            setupDataChannelEvents(dc, targetSid);
            dataChannels[targetSid] = dc;
        };
    }
}

function setupDataChannelEvents(dc, targetSid) {
    dc.binaryType = 'arraybuffer';
    
    dc.onopen = () => {
        logEvent(`P2P DataChannel OPENED with Peer: ${targetSid.substring(0, 8)}...`);
        requestMissingChunks();
    };

    dc.onmessage = (event) => {
        if (typeof event.data === 'string') {
            const msg = JSON.parse(event.data);
            if (msg.type === 'request_chunk') {
                sendChunkToPeer(targetSid, msg.chunkIndex);
            }
        } else {
            handleIncomingChunkBuffer(event.data, targetSid);
        }
    };

    dc.onclose = () => {
        logEvent(`DataChannel CLOSED with Peer: ${targetSid.substring(0, 8)}...`);
    };
}

// --- SIGNALING HANDLERS ---
window.handleIncomingOffer = async function(fromSid, offer) {
    createPeerConnection(fromSid, false);
    const pc = peerConnections[fromSid];
    await pc.setRemoteDescription(new RTCSessionDescription(offer));
    const answer = await pc.createAnswer();
    await pc.setLocalDescription(answer);
    socket.emit('signal_answer', { target: fromSid, answer: pc.localDescription });
};

window.handleIncomingAnswer = async function(fromSid, answer) {
    const pc = peerConnections[fromSid];
    if (pc) {
        await pc.setRemoteDescription(new RTCSessionDescription(answer));
    }
};

window.handleIncomingICE = async function(fromSid, candidate) {
    const pc = peerConnections[fromSid];
    if (pc) {
        await pc.addIceCandidate(new RTCIceCandidate(candidate));
    }
};

// --- CHUNK TRANSFER ---
function requestMissingChunks() {
    const openPeerSids = Object.keys(dataChannels).filter(sid => dataChannels[sid].readyState === 'open');
    if (openPeerSids.length === 0) return;

    let peerIndex = 0;
    for (let i = 0; i < targetTotalChunks; i++) {
        if (!receivedChunks.hasOwnProperty(i)) {
            const targetSid = openPeerSids[peerIndex % openPeerSids.length];
            const dc = dataChannels[targetSid];
            if (dc && dc.readyState === 'open') {
                dc.send(JSON.stringify({ type: 'request_chunk', chunkIndex: i }));
                peerIndex++;
            }
        }
    }
}

function sendChunkToPeer(targetSid, chunkIndex) {
    const dc = dataChannels[targetSid];
    if (dc && dc.readyState === 'open' && localFileChunks[chunkIndex]) {
        const chunkBuffer = localFileChunks[chunkIndex];
        const header = new Int32Array([chunkIndex]);
        const packet = new Uint8Array(header.byteLength + chunkBuffer.byteLength);
        
        packet.set(new Uint8Array(header.buffer), 0);
        packet.set(new Uint8Array(chunkBuffer), header.byteLength);
        dc.send(packet.buffer);
    }
}

let peerChunkCounters = {};
function handleIncomingChunkBuffer(buffer, senderSid) {
    const header = new Int32Array(buffer.slice(0, 4));
    const chunkIndex = header[0];
    const chunkData = buffer.slice(4);

    if (!receivedChunks.hasOwnProperty(chunkIndex)) {
        receivedChunks[chunkIndex] = chunkData;

        peerChunkCounters[senderSid] = (peerChunkCounters[senderSid] || 0) + 1;
        const peerChunksReceived = peerChunkCounters[senderSid];
        const peerPercentage = ((peerChunksReceived / targetTotalChunks) * 100).toFixed(1);
        
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
    localFileChunks = [];
    
    for (let i = 0; i < targetTotalChunks; i++) {
        orderedBuffers.push(receivedChunks[i]);
        localFileChunks.push(receivedChunks[i]);
    }
    
    const blob = new Blob(orderedBuffers);
    const downloadUrl = URL.createObjectURL(blob);
    
    const a = document.createElement('a');
    a.href = downloadUrl;
    a.download = targetFilename;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    logEvent(`File "${targetFilename}" successfully saved to local machine & ready for seeding.`);
}

async function computeSimpleHash(input) {
    const encoder = new TextEncoder();
    const data = encoder.encode(input);
    const hashBuffer = await crypto.subtle.digest('SHA-256', data);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}