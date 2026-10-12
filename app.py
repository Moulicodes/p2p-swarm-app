from flask import Flask, render_template, request
from flask_socketio import SocketIO, emit, join_room, leave_room
import random
import string

app = Flask(__name__)
app.config['SECRET_KEY'] = 'p2p_swarm_secret'
socketio = SocketIO(app, cors_allowed_origins="*")

# Memory Stores
# files_registry: { file_hash: { 'filename': str, 'total_chunks': int, 'peers': { sid: { 'type': str, 'has_chunks': [] } } } }
files_registry = {}
# peer_display_names: { sid: "Node-A41F" }
peer_names = {}

def generate_friendly_name():
    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=4))
    return f"Node-{suffix}"

@app.route('/')
def index():
    return render_template('index.html')

@socketio.on('connect')
def handle_connect():
    friendly_name = generate_friendly_name()
    peer_names[request.sid] = friendly_name
    print(f"[Tracker] Peer connected: {request.sid} ({friendly_name})")
    emit('assigned_peer_name', {'sid': request.sid, 'friendly_name': friendly_name})

@socketio.on('disconnect')
def handle_disconnect():
    friendly_name = peer_names.get(request.sid, request.sid[:8])
    print(f"[Tracker] Peer disconnected: {friendly_name}")
    
    for file_hash, file_info in files_registry.items():
        if request.sid in file_info['peers']:
            del file_info['peers'][request.sid]
            emit('peer_left', {'sid': request.sid, 'friendly_name': friendly_name}, to=file_hash)
            
    if request.sid in peer_names:
        del peer_names[request.sid]

@socketio.on('register_file')
def handle_register_file(data):
    file_hash = data['file_hash']
    filename = data['filename']
    total_chunks = data['total_chunks']
    
    if file_hash not in files_registry:
        files_registry[file_hash] = {
            'filename': filename,
            'total_chunks': total_chunks,
            'peers': {}
        }
    
    files_registry[file_hash]['peers'][request.sid] = {
        'type': 'seeder',
        'has_chunks': list(range(total_chunks))
    }
    
    join_room(file_hash)
    friendly_name = peer_names.get(request.sid, request.sid[:8])
    print(f"[Tracker] File registered: '{filename}' ({file_hash[:8]}...) by {friendly_name}")
    
    emit('file_registered_ack', {
        'status': 'success',
        'file_hash': file_hash,
        'filename': filename,
        'total_chunks': total_chunks
    })
    
    # Broadcast catalog update to all peers
    broadcast_swarm_catalog()

@socketio.on('join_swarm')
def handle_join_swarm(data):
    file_hash = data['file_hash']
    friendly_name = peer_names.get(request.sid, request.sid[:8])
    
    if file_hash in files_registry:
        join_room(file_hash)
        
        files_registry[file_hash]['peers'][request.sid] = {
            'type': 'leecher',
            'has_chunks': []
        }
        
        active_peers = [
            {
                'sid': sid,
                'friendly_name': peer_names.get(sid, sid[:8]),
                'type': info['type'],
                'has_chunks': info['has_chunks']
            }
            for sid, info in files_registry[file_hash]['peers'].items()
            if sid != request.sid
        ]
        
        emit('swarm_info', {
            'file_hash': file_hash,
            'file_info': {
                'filename': files_registry[file_hash]['filename'],
                'total_chunks': files_registry[file_hash]['total_chunks']
            },
            'peers': active_peers
        })
        
        emit('peer_joined', {
            'sid': request.sid,
            'friendly_name': friendly_name,
            'type': 'leecher'
        }, to=file_hash, include_self=False)
        
        broadcast_swarm_catalog()
    else:
        emit('swarm_error', {'message': 'File manifest not found on tracker.'})

@socketio.on('get_swarm_catalog')
def handle_get_swarm_catalog():
    send_swarm_catalog(request.sid)

def broadcast_swarm_catalog():
    catalog = build_catalog_list()
    socketio.emit('catalog_update', {'catalog': catalog})

def send_swarm_catalog(target_sid):
    catalog = build_catalog_list()
    emit('catalog_update', {'catalog': catalog}, to=target_sid)

def build_catalog_list():
    catalog = []
    for f_hash, info in files_registry.items():
        seeders_count = sum(1 for p in info['peers'].values() if p['type'] == 'seeder')
        leechers_count = sum(1 for p in info['peers'].values() if p['type'] == 'leecher')
        catalog.append({
            'file_hash': f_hash,
            'filename': info['filename'],
            'total_chunks': info['total_chunks'],
            'seeders': seeders_count,
            'leechers': leechers_count
        })
    return catalog

# Signaling Relays
@socketio.on('signal_offer')
def handle_offer(data):
    emit('signal_offer', {'from': request.sid, 'offer': data['offer']}, to=data['target'])

@socketio.on('signal_answer')
def handle_answer(data):
    emit('signal_answer', {'from': request.sid, 'answer': data['answer']}, to=data['target'])

@socketio.on('signal_ice')
def handle_ice(data):
    emit('signal_ice', {'from': request.sid, 'candidate': data['candidate']}, to=data['target'])

if __name__ == '__main__':
    socketio.run(app, debug=True, port=5000)