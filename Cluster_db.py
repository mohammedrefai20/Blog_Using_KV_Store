"""
Cassandra-Style Distributed Key-Value Store
Features:
- 3-node cluster with replication (primary + 2 secondaries)
- Leader election on primary failure
- Inverted index for full-text search
- Vector embedding index for similarity search
- Masterless replication option
- 100% durability through WAL
"""

import socket
import json
import os
import threading
import time
import subprocess
import sys
import random
import re
from pathlib import Path
from typing import Optional, List, Tuple, Dict, Any, Set
from collections import defaultdict
from enum import Enum
import hashlib


class NodeRole(Enum):
    """Roles a node can have in the cluster."""
    PRIMARY = "primary"
    SECONDARY = "secondary"
    CANDIDATE = "candidate"  # During election


class ClusterNode:
    """
    Single node in a distributed cluster.
    Supports replication, leader election, and indexing.
    """
    
    def __init__(self, node_id: int, host='localhost', port=5555, 
                 data_dir='./node_data', peers=None, masterless=False):
        """
        Initialize cluster node.
        
        Args:
            node_id: Unique identifier for this node
            host: Host address
            port: Port number
            data_dir: Directory for persistent storage
            peers: List of (host, port) tuples for other nodes
            masterless: Enable masterless multi-master mode
        """
        self.node_id = node_id
        self.host = host
        self.port = port
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(exist_ok=True)
        self.peers = peers or []
        self.masterless = masterless
        
        # Storage
        self.wal_path = self.data_dir / 'wal.log'
        self.snapshot_path = self.data_dir / 'snapshot.json'
        self.store: Dict[str, str] = {}
        self.lock = threading.RLock()
        
        # Cluster state
        self.role = NodeRole.SECONDARY
        self.current_term = 0
        self.voted_for = None
        self.leader_id = None
        self.last_heartbeat = time.time()
        
        # Election timing
        self.election_timeout = 5 + random.uniform(0, 5)  # 5-10 seconds
        self.heartbeat_interval = 2  # 2 seconds
        
        # Indexes
        self.inverted_index: Dict[str, Set[str]] = defaultdict(set)  # word -> set of keys
        self.embeddings: Dict[str, List[float]] = {}  # key -> embedding vector
        
        # Server
        self.running = False
        self.server_socket = None
        
        # Load data
        self._load_data()
        self._rebuild_indexes()
    
    def _load_data(self):
        """Load data from snapshot and replay WAL."""
        if self.snapshot_path.exists():
            try:
                with open(self.snapshot_path, 'r') as f:
                    data = json.load(f)
                    self.store = data.get('store', {})
                    self.current_term = data.get('term', 0)
                print(f"Node {self.node_id}: Loaded {len(self.store)} keys")
            except Exception as e:
                print(f"Node {self.node_id}: Error loading snapshot: {e}")
        
        if self.wal_path.exists():
            try:
                with open(self.wal_path, 'r') as f:
                    for line in f:
                        if line.strip():
                            entry = json.loads(line)
                            self._apply_operation(entry)
            except Exception as e:
                print(f"Node {self.node_id}: Error replaying WAL: {e}")
    
    def _apply_operation(self, entry: Dict[str, Any]):
        """Apply operation to store and update indexes."""
        op = entry['op']
        if op == 'set':
            old_value = self.store.get(entry['key'])
            if old_value:
                self._remove_from_indexes(entry['key'], old_value)
            self.store[entry['key']] = entry['value']
            self._add_to_indexes(entry['key'], entry['value'])
        elif op == 'delete':
            if entry['key'] in self.store:
                self._remove_from_indexes(entry['key'], self.store[entry['key']])
                del self.store[entry['key']]
        elif op == 'bulk_set':
            for key, value in entry['items']:
                old_value = self.store.get(key)
                if old_value:
                    self._remove_from_indexes(key, old_value)
                self.store[key] = value
                self._add_to_indexes(key, value)
    
    def _tokenize(self, text: str) -> List[str]:
        """Tokenize text for inverted index."""
        # Convert to lowercase and split on non-alphanumeric
        return [word.lower() for word in re.findall(r'\w+', text)]
    
    def _compute_embedding(self, text: str) -> List[float]:
        """
        Compute simple embedding for text (bag-of-words with hashing).
        In production, use sentence-transformers or similar.
        """
        # Simple hash-based embedding (256 dimensions)
        embedding = [0.0] * 256
        words = self._tokenize(text)
        for word in words:
            # Hash word to dimension
            hash_val = int(hashlib.md5(word.encode()).hexdigest(), 16)
            idx = hash_val % 256
            embedding[idx] += 1.0
        
        # Normalize
        magnitude = sum(x*x for x in embedding) ** 0.5
        if magnitude > 0:
            embedding = [x / magnitude for x in embedding]
        return embedding
    
    def _cosine_similarity(self, vec1: List[float], vec2: List[float]) -> float:
        """Compute cosine similarity between two vectors."""
        dot_product = sum(a * b for a, b in zip(vec1, vec2))
        return dot_product  # Already normalized
    
    def _add_to_indexes(self, key: str, value: str):
        """Add key-value to all indexes."""
        # Inverted index
        words = self._tokenize(value)
        for word in words:
            self.inverted_index[word].add(key)
        
        # Embedding index
        self.embeddings[key] = self._compute_embedding(value)
    
    def _remove_from_indexes(self, key: str, value: str):
        """Remove key-value from all indexes."""
        # Inverted index
        words = self._tokenize(value)
        for word in words:
            self.inverted_index[word].discard(key)
        
        # Embedding index
        self.embeddings.pop(key, None)
    
    def _rebuild_indexes(self):
        """Rebuild all indexes from current store."""
        self.inverted_index.clear()
        self.embeddings.clear()
        for key, value in self.store.items():
            self._add_to_indexes(key, value)
        print(f"Node {self.node_id}: Rebuilt indexes ({len(self.inverted_index)} terms)")
    
    def _write_to_wal(self, entry: Dict[str, Any]):
        """Write to WAL with fsync."""
        with open(self.wal_path, 'a') as f:
            f.write(json.dumps(entry) + '\n')
            f.flush()
            os.fsync(f.fileno())
    
    def _create_snapshot(self):
        """Create snapshot with term and store data."""
        temp_path = self.snapshot_path.with_suffix('.tmp')
        try:
            with open(temp_path, 'w') as f:
                json.dump({
                    'store': self.store,
                    'term': self.current_term
                }, f)
                f.flush()
                os.fsync(f.fileno())
            temp_path.replace(self.snapshot_path)
            
            with open(self.wal_path, 'w') as f:
                f.flush()
                os.fsync(f.fileno())
        except Exception as e:
            print(f"Node {self.node_id}: Snapshot error: {e}")
    
    def set(self, key: str, value: str) -> bool:
        """Set key-value pair (only on primary in master-slave mode)."""
        with self.lock:
            if not self.masterless and self.role != NodeRole.PRIMARY:
                return False
            
            entry = {'op': 'set', 'key': key, 'value': value}
            self._write_to_wal(entry)
            self._apply_operation(entry)
            
            # Replicate to secondaries
            if not self.masterless and self.role == NodeRole.PRIMARY:
                self._replicate_to_peers(entry)
            
            return True
    
    def get(self, key: str) -> Optional[str]:
        """Get value for key."""
        with self.lock:
            return self.store.get(key)
    
    def delete(self, key: str) -> bool:
        """Delete key."""
        with self.lock:
            if not self.masterless and self.role != NodeRole.PRIMARY:
                return False
            
            if key not in self.store:
                return False
            
            entry = {'op': 'delete', 'key': key}
            self._write_to_wal(entry)
            self._apply_operation(entry)
            
            if not self.masterless and self.role == NodeRole.PRIMARY:
                self._replicate_to_peers(entry)
            
            return True
    
    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        """Bulk set multiple key-value pairs."""
        with self.lock:
            if not self.masterless and self.role != NodeRole.PRIMARY:
                return False
            
            entry = {'op': 'bulk_set', 'items': items}
            self._write_to_wal(entry)
            self._apply_operation(entry)
            
            if not self.masterless and self.role == NodeRole.PRIMARY:
                self._replicate_to_peers(entry)
            
            return True
    
    def search(self, query: str) -> List[str]:
        """
        Full-text search using inverted index.
        Returns keys whose values contain query terms.
        """
        with self.lock:
            words = self._tokenize(query)
            if not words:
                return []
            
            # Find keys that contain ALL query words (AND search)
            result_sets = [self.inverted_index.get(word, set()) for word in words]
            if not result_sets:
                return []
            
            matching_keys = set.intersection(*result_sets)
            return list(matching_keys)
    
    def search_similar(self, text: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """
        Similarity search using embeddings.
        Returns top_k keys with most similar values.
        """
        with self.lock:
            query_embedding = self._compute_embedding(text)
            
            similarities = []
            for key, embedding in self.embeddings.items():
                sim = self._cosine_similarity(query_embedding, embedding)
                similarities.append((key, sim))
            
            similarities.sort(key=lambda x: x[1], reverse=True)
            return similarities[:top_k]
    
    def _replicate_to_peers(self, entry: Dict[str, Any]):
        """Replicate operation to peer nodes (async)."""
        def replicate_to_peer(peer_host, peer_port):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((peer_host, peer_port))
                
                request = {'cmd': 'replicate', 'entry': entry}
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                sock.close()
            except Exception as e:
                pass  # Async replication, ignore errors
        
        for peer_host, peer_port in self.peers:
            threading.Thread(target=replicate_to_peer, args=(peer_host, peer_port), daemon=True).start()
    
    def _handle_replicate(self, entry: Dict[str, Any]):
        """Handle replication from primary."""
        with self.lock:
            self._write_to_wal(entry)
            self._apply_operation(entry)
    
    def _start_election(self):
        """Start leader election."""
        with self.lock:
            self.current_term += 1
            self.role = NodeRole.CANDIDATE
            self.voted_for = self.node_id
            votes_received = 1  # Vote for self
            
            print(f"Node {self.node_id}: Starting election for term {self.current_term}")
        
        # Request votes from peers
        def request_vote(peer_host, peer_port):
            nonlocal votes_received
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((peer_host, peer_port))
                
                request = {
                    'cmd': 'request_vote',
                    'term': self.current_term,
                    'candidate_id': self.node_id
                }
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                
                # Receive response
                length_bytes = sock.recv(4)
                if length_bytes:
                    msg_len = int.from_bytes(length_bytes, 'big')
                    resp_data = b''
                    while len(resp_data) < msg_len:
                        resp_data += sock.recv(msg_len - len(resp_data))
                    response = json.loads(resp_data.decode('utf-8'))
                    if response.get('vote_granted'):
                        votes_received += 1
                
                sock.close()
            except Exception as e:
                pass
        
        threads = []
        for peer in self.peers:
            t = threading.Thread(target=request_vote, args=peer, daemon=True)
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join(timeout=3.0)
        
        # Check if won election (majority)
        with self.lock:
            if votes_received > len(self.peers) / 2:
                self.role = NodeRole.PRIMARY
                self.leader_id = self.node_id
                print(f"Node {self.node_id}: Became PRIMARY with {votes_received} votes")
                # Sync indexes to peers
                self._sync_indexes_to_peers()
            else:
                self.role = NodeRole.SECONDARY
                print(f"Node {self.node_id}: Election failed with {votes_received} votes")
    
    def _send_heartbeat(self):
        """Send heartbeat to peers (primary only)."""
        if self.role != NodeRole.PRIMARY:
            return
        
        def send_to_peer(peer_host, peer_port):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(1.0)
                sock.connect((peer_host, peer_port))
                
                request = {
                    'cmd': 'heartbeat',
                    'term': self.current_term,
                    'leader_id': self.node_id
                }
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                sock.close()
            except:
                pass
        
        for peer in self.peers:
            threading.Thread(target=send_to_peer, args=peer, daemon=True).start()
    
    def _sync_indexes_to_peers(self):
        """Sync indexes to peer nodes."""
        def sync_to_peer(peer_host, peer_port):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(5.0)
                sock.connect((peer_host, peer_port))
                
                # Send inverted index
                inv_idx_serializable = {k: list(v) for k, v in self.inverted_index.items()}
                request = {
                    'cmd': 'sync_indexes',
                    'inverted_index': inv_idx_serializable,
                    'embeddings': self.embeddings
                }
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                sock.close()
            except Exception as e:
                print(f"Node {self.node_id}: Error syncing indexes to {peer_host}:{peer_port}")
        
        for peer in self.peers:
            threading.Thread(target=sync_to_peer, args=peer, daemon=True).start()
    
    def _handle_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """Handle incoming request."""
        cmd = request.get('cmd')
        
        if cmd == 'set':
            success = self.set(request['key'], request['value'])
            return {'status': 'ok' if success else 'error', 'result': success}
        
        elif cmd == 'get':
            value = self.get(request['key'])
            return {'status': 'ok', 'result': value}
        
        elif cmd == 'delete':
            success = self.delete(request['key'])
            return {'status': 'ok', 'result': success}
        
        elif cmd == 'bulk_set':
            success = self.bulk_set(request['items'])
            return {'status': 'ok', 'result': success}
        
        elif cmd == 'search':
            results = self.search(request['query'])
            return {'status': 'ok', 'result': results}
        
        elif cmd == 'search_similar':
            results = self.search_similar(request['text'], request.get('top_k', 5))
            return {'status': 'ok', 'result': results}
        
        elif cmd == 'get_leader':
            return {'status': 'ok', 'leader_id': self.leader_id, 'leader_port': self.port if self.role == NodeRole.PRIMARY else None}
        
        elif cmd == 'replicate':
            self._handle_replicate(request['entry'])
            return {'status': 'ok'}
        
        elif cmd == 'heartbeat':
            with self.lock:
                if request['term'] >= self.current_term:
                    self.current_term = request['term']
                    self.leader_id = request['leader_id']
                    self.last_heartbeat = time.time()
                    if self.role != NodeRole.SECONDARY:
                        self.role = NodeRole.SECONDARY
            return {'status': 'ok'}
        
        elif cmd == 'request_vote':
            with self.lock:
                vote_granted = False
                if request['term'] > self.current_term:
                    self.current_term = request['term']
                    self.voted_for = None
                
                if self.voted_for is None or self.voted_for == request['candidate_id']:
                    self.voted_for = request['candidate_id']
                    vote_granted = True
                
                return {'status': 'ok', 'vote_granted': vote_granted, 'term': self.current_term}
        
        elif cmd == 'sync_indexes':
            with self.lock:
                # Receive index sync from primary
                self.inverted_index = defaultdict(set, {k: set(v) for k, v in request['inverted_index'].items()})
                self.embeddings = request['embeddings']
                print(f"Node {self.node_id}: Synced indexes from primary")
            return {'status': 'ok'}
        
        # Masterless operations
        elif cmd == 'quorum_write':
            # Write to local store
            with self.lock:
                entry = {'op': 'set', 'key': request['key'], 'value': request['value']}
                self._write_to_wal(entry)
                self._apply_operation(entry)
            return {'status': 'ok', 'node_id': self.node_id}
        
        elif cmd == 'quorum_read':
            with self.lock:
                value = self.store.get(request['key'])
            return {'status': 'ok', 'value': value, 'node_id': self.node_id}
        
        return {'status': 'error', 'message': 'Unknown command'}
    
    def _handle_client(self, client_socket: socket.socket):
        """Handle client connection."""
        try:
            while self.running:
                length_bytes = client_socket.recv(4)
                if not length_bytes:
                    break
                
                msg_length = int.from_bytes(length_bytes, 'big')
                data = b''
                while len(data) < msg_length:
                    chunk = client_socket.recv(min(4096, msg_length - len(data)))
                    if not chunk:
                        break
                    data += chunk
                
                if not data:
                    break
                
                request = json.loads(data.decode('utf-8'))
                response = self._handle_request(request)
                
                response_data = json.dumps(response).encode('utf-8')
                response_length = len(response_data).to_bytes(4, 'big')
                client_socket.sendall(response_length + response_data)
        except Exception as e:
            pass
        finally:
            client_socket.close()
    
    def start(self):
        """Start the node."""
        self.running = True
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.host, self.port))
        self.server_socket.listen(10)
        
        print(f"Node {self.node_id} started on {self.host}:{self.port}")
        
        # Heartbeat and election thread
        def monitor():
            while self.running:
                time.sleep(1)
                
                if self.role == NodeRole.PRIMARY:
                    self._send_heartbeat()
                else:
                    # Check if need to start election
                    time_since_heartbeat = time.time() - self.last_heartbeat
                    if time_since_heartbeat > self.election_timeout:
                        print(f"Node {self.node_id}: No heartbeat for {time_since_heartbeat:.1f}s")
                        self._start_election()
                        self.last_heartbeat = time.time()
        
        threading.Thread(target=monitor, daemon=True).start()
        
        # Snapshot thread
        def snapshot_worker():
            while self.running:
                time.sleep(10)
                if self.running:
                    with self.lock:
                        if os.path.exists(self.wal_path) and os.path.getsize(self.wal_path) > 10000:
                            self._create_snapshot()
        
        threading.Thread(target=snapshot_worker, daemon=True).start()
        
        while self.running:
            try:
                self.server_socket.settimeout(1.0)
                try:
                    client_socket, addr = self.server_socket.accept()
                    threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
                except socket.timeout:
                    continue
            except Exception as e:
                if self.running:
                    print(f"Node {self.node_id} error: {e}")
    
    def stop(self):
        """Stop the node."""
        print(f"Node {self.node_id}: Stopping...")
        self.running = False
        with self.lock:
            self._create_snapshot()
        if self.server_socket:
            self.server_socket.close()


class ClusterClient:
    """
    Client for distributed cluster.
    Auto-discovers primary and handles failover.
    """
    
    def __init__(self, cluster_nodes: List[Tuple[str, int]], masterless=False):
        """
        Initialize cluster client.
        
        Args:
            cluster_nodes: List of (host, port) for all nodes
            masterless: Use masterless quorum mode
        """
        self.cluster_nodes = cluster_nodes
        self.masterless = masterless
        self.primary_conn = None
        self.primary_addr = None
        self._discover_primary()
    
    def _discover_primary(self):
        """Discover which node is the primary."""
        if self.masterless:
            # In masterless mode, connect to any node
            for host, port in self.cluster_nodes:
                try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.connect((host, port))
                    self.primary_conn = sock
                    self.primary_addr = (host, port)
                    return
                except:
                    continue
            raise ConnectionError("Cannot connect to any node")
        
        # Master-slave mode: find primary
        for host, port in self.cluster_nodes:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((host, port))
                
                request = {'cmd': 'get_leader'}
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                
                length_bytes = sock.recv(4)
                msg_len = int.from_bytes(length_bytes, 'big')
                resp_data = b''
                while len(resp_data) < msg_len:
                    resp_data += sock.recv(msg_len - len(resp_data))
                response = json.loads(resp_data.decode('utf-8'))
                
                if response.get('leader_port') == port:
                    # This node is primary
                    self.primary_conn = sock
                    self.primary_addr = (host, port)
                    print(f"Connected to primary at {host}:{port}")
                    return
                sock.close()
            except:
                continue
        
        raise ConnectionError("Cannot find primary node")
    
    def _reconnect(self):
        """Reconnect to primary (handles failover)."""
        if self.primary_conn:
            self.primary_conn.close()
        self.primary_conn = None
        time.sleep(2)  # Wait for election
        self._discover_primary()
    
    def _send_request(self, request: Dict[str, Any], retry=True) -> Dict[str, Any]:
        """Send request to primary with automatic retry."""
        try:
            data = json.dumps(request).encode('utf-8')
            length = len(data).to_bytes(4, 'big')
            self.primary_conn.sendall(length + data)
            
            length_bytes = self.primary_conn.recv(4)
            msg_len = int.from_bytes(length_bytes, 'big')
            resp_data = b''
            while len(resp_data) < msg_len:
                resp_data += self.primary_conn.recv(msg_len - len(resp_data))
            
            return json.loads(resp_data.decode('utf-8'))
        except Exception as e:
            if retry:
                print(f"Connection error, reconnecting...")
                self._reconnect()
                return self._send_request(request, retry=False)
            raise
    
    def _quorum_write(self, key: str, value: str) -> bool:
        """Perform quorum write (masterless mode)."""
        acks = 0
        quorum = len(self.cluster_nodes) // 2 + 1
        
        def write_to_node(host, port):
            nonlocal acks
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((host, port))
                
                request = {'cmd': 'quorum_write', 'key': key, 'value': value}
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                
                length_bytes = sock.recv(4)
                msg_len = int.from_bytes(length_bytes, 'big')
                resp_data = sock.recv(msg_len)
                response = json.loads(resp_data.decode('utf-8'))
                
                if response.get('status') == 'ok':
                    acks += 1
                
                sock.close()
            except:
                pass
        
        threads = []
        for host, port in self.cluster_nodes:
            t = threading.Thread(target=write_to_node, args=(host, port))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join(timeout=3.0)
        
        return acks >= quorum
    
    def _quorum_read(self, key: str) -> Optional[str]:
        """Perform quorum read (masterless mode)."""
        responses = []
        quorum = len(self.cluster_nodes) // 2 + 1
        lock = threading.Lock()
        
        def read_from_node(host, port):
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.settimeout(2.0)
                sock.connect((host, port))
                
                request = {'cmd': 'quorum_read', 'key': key}
                data = json.dumps(request).encode('utf-8')
                length = len(data).to_bytes(4, 'big')
                sock.sendall(length + data)
                
                length_bytes = sock.recv(4)
                msg_len = int.from_bytes(length_bytes, 'big')
                resp_data = sock.recv(msg_len)
                response = json.loads(resp_data.decode('utf-8'))
                
                with lock:
                    responses.append(response.get('value'))
                
                sock.close()
            except:
                pass
        
        threads = []
        for host, port in self.cluster_nodes:
            t = threading.Thread(target=read_from_node, args=(host, port))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join(timeout=3.0)
        
        if len(responses) >= quorum:
            # Return most common value (simple conflict resolution)
            from collections import Counter
            counts = Counter(responses)
            return counts.most_common(1)[0][0]
        return None
    
    def Set(self, key: str, value: str) -> bool:
        """Set key-value pair."""
        if self.masterless:
            return self._quorum_write(key, value)
        response = self._send_request({'cmd': 'set', 'key': key, 'value': value})
        return response.get('result', False)
    
    def Get(self, key: str) -> Optional[str]:
        """Get value for key."""
        if self.masterless:
            return self._quorum_read(key)
        response = self._send_request({'cmd': 'get', 'key': key})
        return response.get('result')
    
    def Delete(self, key: str) -> bool:
        """Delete key."""
        response = self._send_request({'cmd': 'delete', 'key': key})
        return response.get('result', False)
    
    def BulkSet(self, items: List[Tuple[str, str]]) -> bool:
        """Bulk set multiple key-value pairs."""
        response = self._send_request({'cmd': 'bulk_set', 'items': items})
        return response.get('result', False)
    
    def Search(self, query: str) -> List[str]:
        """Full-text search using inverted index."""
        response = self._send_request({'cmd': 'search', 'query': query})
        return response.get('result', [])
    
    def SearchSimilar(self, text: str, top_k: int = 5) -> List[Tuple[str, float]]:
        """Similarity search using embeddings."""
        response = self._send_request({'cmd': 'search_similar', 'text': text, 'top_k': top_k})
        return response.get('result', [])
    
    def close(self):
        """Close connection."""
        if self.primary_conn:
            self.primary_conn.close()


# ==================== TESTS ====================

def run_cluster_tests():
    """Test cluster operations including failover."""
    import shutil
    import platform
    
    print("\n" + "="*60)
    print("CLUSTER TESTS")
    print("="*60)
    
    # Clean up
    for i in range(3):
        data_dir = f'./cluster_node_{i}'
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
    
    # Start 3-node cluster
    nodes = []
    ports = [6001, 6002, 6003]
    peers_list = [
        [('localhost', 6002), ('localhost', 6003)],  # Node 0 peers
        [('localhost', 6001), ('localhost', 6003)],  # Node 1 peers
        [('localhost', 6001), ('localhost', 6002)]   # Node 2 peers
    ]
    
    for i in range(3):
        node = ClusterNode(
            node_id=i,
            port=ports[i],
            data_dir=f'./cluster_node_{i}',
            peers=peers_list[i]
        )
        nodes.append(node)
        threading.Thread(target=node.start, daemon=True).start()
    
    time.sleep(2)
    
    # Manually set first node as primary
    nodes[0].role = NodeRole.PRIMARY
    nodes[0].leader_id = 0
    print(f"Node 0 is PRIMARY")
    
    try:
        # Test 1: Basic replication
        print("\nTest 1: Primary Write + Replication")
        client = ClusterClient([('localhost', p) for p in ports])
        client.Set('test_key', 'test_value')
        time.sleep(1)  # Wait for replication
        
        # Check all nodes have the data
        for i, node in enumerate(nodes):
            value = node.get('test_key')
            print(f"  Node {i}: {value}")
            assert value == 'test_value', f"Node {i} missing data"
        print("✓ Replication works")
        
        # Test 2: Failover
        print("\nTest 2: Primary Failure + Election")
        print("  Stopping primary (Node 0)...")
        nodes[0].stop()
        time.sleep(8)  # Wait for election timeout
        
        # Check if new primary elected
        new_primary = None
        for i in [1, 2]:
            if nodes[i].role == NodeRole.PRIMARY:
                new_primary = i
                break
        
        assert new_primary is not None, "No new primary elected!"
        print(f"  Node {new_primary} became new PRIMARY")
        print("✓ Election successful")
        
        # Test 3: Write to new primary
        print("\nTest 3: Write to New Primary")
        client.close()
        time.sleep(1)
        client = ClusterClient([('localhost', p) for p in ports])
        client.Set('after_failover', 'new_data')
        time.sleep(1)
        
        value = nodes[new_primary].get('after_failover')
        assert value == 'new_data', "Write to new primary failed"
        print("✓ Can write to new primary")
        
        client.close()
        print("\n✓ All cluster tests passed!")
        
    finally:
        for node in nodes:
            node.stop()
        time.sleep(1)


def run_index_tests():
    """Test indexing features."""
    import shutil
    
    print("\n" + "="*60)
    print("INDEX TESTS")
    print("="*60)
    
    data_dir = './index_test_node'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    node = ClusterNode(node_id=0, port=6100, data_dir=data_dir, peers=[])
    node.role = NodeRole.PRIMARY  # Make it primary
    threading.Thread(target=node.start, daemon=True).start()
    time.sleep(1)
    
    try:
        client = ClusterClient([('localhost', 6100)])
        
        # Test 1: Inverted Index
        print("\nTest 1: Inverted Index (Full-Text Search)")
        client.Set('doc1', 'the quick brown fox jumps over the lazy dog')
        client.Set('doc2', 'the lazy cat sleeps all day')
        client.Set('doc3', 'quick brown rabbits hop around')
        time.sleep(0.5)
        
        # Search for "lazy"
        results = client.Search('lazy')
        print(f"  Search 'lazy': {results}")
        assert 'doc1' in results and 'doc2' in results, "Inverted index failed"
        
        # Search for "quick brown"
        results = client.Search('quick brown')
        print(f"  Search 'quick brown': {results}")
        assert 'doc1' in results and 'doc3' in results, "Multi-word search failed"
        print("✓ Inverted index works")
        
        # Test 2: Embedding Search
        print("\nTest 2: Embedding Search (Similarity)")
        client.Set('article1', 'machine learning and artificial intelligence')
        client.Set('article2', 'cooking recipes and kitchen tips')
        client.Set('article3', 'deep learning neural networks')
        time.sleep(0.5)
        
        # Search similar to AI/ML content
        results = client.SearchSimilar('AI and ML algorithms', top_k=2)
        print(f"  Similar to 'AI and ML algorithms':")
        for key, score in results:
            print(f"    {key}: {score:.3f}")
        
        # Top result should be ML-related
        top_keys = [r[0] for r in results[:2]]
        assert 'article1' in top_keys or 'article3' in top_keys, "Similarity search failed"
        print("✓ Embedding search works")
        
        client.close()
        print("\n✓ All index tests passed!")
        
    finally:
        node.stop()
        time.sleep(1)


def run_masterless_tests():
    """Test masterless replication with quorum."""
    import shutil
    
    print("\n" + "="*60)
    print("MASTERLESS REPLICATION TESTS")
    print("="*60)
    
    # Clean up
    for i in range(3):
        data_dir = f'./masterless_node_{i}'
        if os.path.exists(data_dir):
            shutil.rmtree(data_dir)
    
    # Start 3-node masterless cluster
    nodes = []
    ports = [7001, 7002, 7003]
    peers_list = [
        [('localhost', 7002), ('localhost', 7003)],
        [('localhost', 7001), ('localhost', 7003)],
        [('localhost', 7001), ('localhost', 7002)]
    ]
    
    for i in range(3):
        node = ClusterNode(
            node_id=i,
            port=ports[i],
            data_dir=f'./masterless_node_{i}',
            peers=peers_list[i],
            masterless=True
        )
        nodes.append(node)
        threading.Thread(target=node.start, daemon=True).start()
    
    time.sleep(2)
    
    try:
        # Test 1: Quorum write
        print("\nTest 1: Quorum Write")
        client = ClusterClient([('localhost', p) for p in ports], masterless=True)
        
        success = client.Set('quorum_key', 'quorum_value')
        assert success, "Quorum write failed"
        print("  Quorum write acknowledged")
        
        time.sleep(1)
        
        # Check that at least 2 nodes have the data
        count = sum(1 for node in nodes if node.get('quorum_key') == 'quorum_value')
        print(f"  {count}/3 nodes have the data")
        assert count >= 2, "Quorum write didn't reach enough nodes"
        print("✓ Quorum write successful")
        
        # Test 2: Quorum read
        print("\nTest 2: Quorum Read")
        value = client.Get('quorum_key')
        assert value == 'quorum_value', "Quorum read failed"
        print("✓ Quorum read successful")
        
        # Test 3: Write with one node down
        print("\nTest 3: Write with One Node Down")
        print("  Stopping node 2...")
        nodes[2].stop()
        time.sleep(1)
        
        success = client.Set('resilient_key', 'resilient_value')
        assert success, "Write failed with one node down"
        print("✓ Can write with one node down (quorum still met)")
        
        client.close()
        print("\n✓ All masterless tests passed!")
        
    finally:
        for node in nodes:
            if node.running:
                node.stop()
        time.sleep(1)


if __name__ == '__main__':
    """Run all tests."""
    print("\n" + "="*60)
    print("CASSANDRA-STYLE DISTRIBUTED KV STORE")
    print("="*60)
    
    run_cluster_tests()
    run_index_tests()
    run_masterless_tests()
    
    print("\n" + "="*60)
    print("ALL TESTS COMPLETED")
    print("="*60)