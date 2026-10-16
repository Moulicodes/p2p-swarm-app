import os
import random
import string
from flask import Flask, render_template, request, redirect, url_for, flash, jsonify
from flask_socketio import SocketIO, emit, join_room, leave_room
from flask_sqlalchemy import SQLAlchemy
from flask_login import LoginManager, UserMixin, login_user, logout_user, login_required, current_user
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.config['SECRET_KEY'] = 'p2p_swarm_secret_key_change_in_prod'

db_url = os.environ.get('DATABASE_URL', 'sqlite:///p2p_swarm.db')
if db_url.startswith("postgres://"):
    db_url = db_url.replace("postgres://", "postgresql+psycopg://", 1)
elif db_url.startswith("postgresql://") and not db_url.startswith("postgresql+psycopg://"):
    db_url = db_url.replace("postgresql://", "postgresql+psycopg://", 1)

app.config['SQLALCHEMY_DATABASE_URI'] = db_url
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False

db = SQLAlchemy(app)
login_manager = LoginManager(app)
login_manager.login_view = 'login'

socketio = SocketIO(app, cors_allowed_origins="*")

# --- DATABASE MODELS ---
class User(UserMixin, db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    email = db.Column(db.String(120), unique=True, nullable=False)
    password_hash = db.Column(db.String(256), nullable=False)
    node_id = db.Column(db.String(20), unique=True, nullable=False)

    def set_password(self, password):
        self.password_hash = generate_password_hash(password)

    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class SeededFile(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    file_hash = db.Column(db.String(64), nullable=False)
    filename = db.Column(db.String(255), nullable=False)
    total_chunks = db.Column(db.Integer, nullable=False)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'), nullable=False)

@login_manager.user_loader
def load_user(user_id):
    return User.query.get(int(user_id))

with app.app_context():
    db.create_all()

active_swarms = {}
socket_to_user = {}

@app.route('/')
def index():
    if not current_user.is_authenticated:
        return render_template('login.html')
    return render_template('index.html')

@app.route('/register', methods=['POST'])
def register():
    data = request.form
    username = data.get('username')
    email = data.get('email')
    password = data.get('password')

    if User.query.filter((User.username == username) | (User.email == email)).first():
        flash('Username or Email already exists.', 'error')
        return redirect(url_for('index'))

    node_id = f"Node-{''.join(random.choices(string.ascii_uppercase + string.digits, k=4))}"
    user = User(username=username, email=email, node_id=node_id)
    user.set_password(password)
    db.session.add(user)
    db.session.commit()

    login_user(user)
    flash('Account created successfully!', 'success')
    return redirect(url_for('index'))

@app.route('/login', methods=['POST'])
def login():
    data = request.form
    username = data.get('username')
    password = data.get('password')

    user = User.query.filter_by(username=username).first()
    if user and user.check_password(password):
        login_user(user)
        flash('Logged in successfully!', 'success')
    else:
        flash('Invalid username or password.', 'error')
    return redirect(url_for('index'))

@app.route('/logout')
@login_required
def logout():
    logout_user()
    flash('Logged out.', 'info')
    return redirect(url_for('index'))

@app.route('/api/user_info')
def user_info():
    if current_user.is_authenticated:
        return jsonify({
            'logged_in': True,
            'user_id': current_user.id,
            'username': current_user.username,
            'node_id': current_user.node_id
        })
    return jsonify({'logged_in': False})

# --- SOCKET.IO SIGNALLING & TRACKER ---
@socketio.on('connect')
def handle_connect():
    if current_user.is_authenticated:
        socket_to_user[request.sid] = {
            'user_id': current_user.id,
            'username': current_user.username,
            'node_id': current_user.node_id
        }
        print(f"[Tracker] User Connected: {current_user.username} ({current_user.node_id})")
        emit('assigned_peer_name', {
            'sid': request.sid,
            'friendly_name': current_user.node_id,
            'username': current_user.username
        })

@socketio.on('disconnect')
def handle_disconnect():
    user_info = socket_to_user.get(request.sid, {})
    friendly_name = user_info.get('node_id', request.sid[:8])
    print(f"[Tracker] Peer Disconnected: {friendly_name}")

    for file_hash, swarm in list(active_swarms.items()):
        if request.sid in swarm.get('peers', {}):
            del swarm['peers'][request.sid]
            emit('peer_left', {'sid': request.sid, 'friendly_name': friendly_name}, to=file_hash)
            if not swarm['peers']:
                del active_swarms[file_hash]

    if request.sid in socket_to_user:
        del socket_to_user[request.sid]

    broadcast_swarm_catalog()

@socketio.on('register_file')
def handle_register_file(data):
    file_hash = data['file_hash']
    filename = data['filename']
    total_chunks = data['total_chunks']

    if current_user.is_authenticated:
        existing = SeededFile.query.filter_by(file_hash=file_hash, user_id=current_user.id).first()
        if not existing:
            db_file = SeededFile(
                file_hash=file_hash,
                filename=filename,
                total_chunks=total_chunks,
                user_id=current_user.id
            )
            db.session.add(db_file)
            db.session.commit()

    if file_hash not in active_swarms:
        active_swarms[file_hash] = {
            'filename': filename,
            'total_chunks': total_chunks,
            'peers': {}
        }

    user_data = socket_to_user.get(request.sid, {})
    node_id = user_data.get('node_id', f"Node-{request.sid[:4]}")

    active_swarms[file_hash]['peers'][request.sid] = {
        'type': 'seeder',
        'node_id': node_id
    }

    join_room(file_hash)
    emit('file_registered_ack', {
        'status': 'success',
        'file_hash': file_hash,
        'filename': filename,
        'total_chunks': total_chunks
    })

    broadcast_swarm_catalog()

@socketio.on('unregister_file')
def handle_unregister_file(data):
    file_hash = data.get('file_hash')
    if file_hash in active_swarms:
        if request.sid in active_swarms[file_hash]['peers']:
            del active_swarms[file_hash]['peers'][request.sid]
            leave_room(file_hash)
            
            if current_user.is_authenticated:
                SeededFile.query.filter_by(file_hash=file_hash, user_id=current_user.id).delete()
                db.session.commit()

            if not active_swarms[file_hash]['peers']:
                del active_swarms[file_hash]

    broadcast_swarm_catalog()
    emit('file_unregistered_ack', {'file_hash': file_hash})

@socketio.on('join_swarm')
def handle_join_swarm(data):
    file_hash = data['file_hash']
    user_data = socket_to_user.get(request.sid, {})
    node_id = user_data.get('node_id', f"Node-{request.sid[:4]}")

    if file_hash in active_swarms:
        join_room(file_hash)

        active_swarms[file_hash]['peers'][request.sid] = {
            'type': 'leecher',
            'node_id': node_id
        }

        active_seeders = [
            {
                'sid': sid,
                'friendly_name': p_info['node_id'],
                'type': p_info['type']
            }
            for sid, p_info in active_swarms[file_hash]['peers'].items()
            if sid != request.sid and p_info['type'] == 'seeder'
        ]

        # Inform the leecher of metadata
        emit('swarm_info', {
            'file_hash': file_hash,
            'file_info': {
                'filename': active_swarms[file_hash]['filename'],
                'total_chunks': active_swarms[file_hash]['total_chunks']
            },
            'peers': active_seeders
        })

        # Directly tell active seeders to initiate WebRTC offers to the leecher
        for seeder in active_seeders:
            emit('connect_to_leecher', {
                'leecher_sid': request.sid,
                'friendly_name': node_id
            }, to=seeder['sid'])

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
    for f_hash, info in active_swarms.items():
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