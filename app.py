from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room

app = Flask(__name__)
app.config['SECRET_KEY'] = 'p2p_swarm_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# In-memory Tracker Store
# Structure: { file_hash: { 'filename': str, 'chunks': int, 'peers': { socket_id: { 'peer_type': 'seeder'/'leecher', 'has_chunks': [] } } } }
files_registry = {}

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    print(f"[Tracker] Peer connected: {request.sid}")

@socketio.on('disconnect')
def handle_disconnect():
    print(f"[Tracker] Peer disconnected: {request.sid}")
    # Remove peer from registered files
    for file_hash, file_info in files_registry.items():
        if request.sid in file_info['peers']:
            del file_info['peers'][request.sid]
            emit('peer_left', {'sid': request.sid}, to=file_hash)

@socketio.on('register_file')
def handle_register_file(data):
    """
    Called by a Seeder to publish a file manifest.
    data = { 'file_hash': str, 'filename': str, 'total_chunks': int }
    """
    file_hash = data['file_hash']
    if file_hash not in files_registry:
        files_registry[file_hash] = {
            'filename': data['filename'],
            'total_chunks': data['total_chunks'],
            'peers': {}
        }
    
    # Register this peer as a seeder holding all chunks
    files_registry[file_hash]['peers'][request.sid] = {
        'type': 'seeder',
        'has_chunks': list(range(data['total_chunks']))
    }
    
    join_room(file_hash)
    print(f"[Tracker] File registered: {data['filename']} ({file_hash[:8]}...) by {request.sid}")
    emit('file_registered_ack', {'status': 'success', 'file_hash': file_hash})

@socketio.on('join_swarm')
def handle_join_swarm(data):
    """
    Called by a Leecher to discover available peers for a file.
    data = { 'file_hash': str }
    """
    file_hash = data['file_hash']
    if file_hash in files_registry:
        join_room(file_hash)
        
        # Register leecher with empty chunks list initially
        files_registry[file_hash]['peers'][request.sid] = {
            'type': 'leecher',
            'has_chunks': []
        }
        
        # Get list of all other peers in swarm
        active_peers = [
            {'sid': sid, 'type': info['type'], 'has_chunks': info['has_chunks']}
            for sid, info in files_registry[file_hash]['peers'].items()
            if sid != request.sid
        ]
        
        emit('swarm_info', {
            'file_info': {
                'filename': files_registry[file_hash]['filename'],
                'total_chunks': files_registry[file_hash]['total_chunks']
            },
            'peers': active_peers
        })
        
        # Notify existing peers that a new peer joined
        emit('peer_joined', {'sid': request.sid, 'type': 'leecher'}, to=file_hash, include_self=False)
    else:
        emit('swarm_error', {'message': 'File manifest not found on tracker.'})

# --- WebRTC Signaling Relays ---

@socketio.on('signal_offer')
def handle_offer(data):
    # Relay WebRTC Offer directly to target peer
    emit('signal_offer', {'from': request.sid, 'offer': data['offer']}, to=data['target'])

@socketio.on('signal_answer')
def handle_answer(data):
    # Relay WebRTC Answer directly to target peer
    emit('signal_answer', {'from': request.sid, 'answer': data['answer']}, to=data['target'])

@socketio.on('signal_ice')
def handle_ice(data):
    # Relay ICE Candidate directly to target peer
    emit('signal_ice', {'from': request.sid, 'candidate': data['candidate']}, to=data['target'])

if __name__ == '__main__':
    print("[Tracker Server Started] Running on http://127.0.0.1:5000")
    socketio.run(app, debug=True, port=5000)