"""
================================================================================
KEY-VALUE STORE EXPLANATION
================================================================================

WHAT IS A KEY-VALUE STORE?
---------------------------
A key-value store is like a dictionary or phone book:
- You store data as pairs: KEY → VALUE
- Example: "name" → "Ahmed", "age" → "25"
- You can: SET (store), GET (retrieve), DELETE (remove) data

WHAT MAKES THIS SPECIAL?
-------------------------
1. PERSISTENCE: Data survives server restarts (saved to disk)
2. NETWORK ACCESS: Clients connect over TCP (like a web server)
3. DURABILITY: Uses Write-Ahead Logging (WAL) - every write is saved immediately
4. HIGH PERFORMANCE: Can handle many writes per second

HOW IT WORKS:
-------------
1. SERVER: Listens on a port (like port 5555), accepts connections
2. CLIENT: Connects to server, sends commands (Set/Get/Delete)
3. PROTOCOL: Uses JSON messages over TCP (like HTTP but simpler)
4. STORAGE: 
   - WAL (Write-Ahead Log): Records every operation immediately
   - Snapshot: Periodic backup of all data (faster restarts)

================================================================================
"""

# ============================================================================
# IMPORTS - Libraries we need
# ============================================================================
import socket      # For TCP networking (sending/receiving data over network)
import json        # For converting Python objects to/from JSON strings
import os          # For file operations (fsync to force disk writes)
import threading   # For handling multiple clients simultaneously
import time        # For timing operations and delays
from pathlib import Path  # For easier file path handling
from typing import Optional, List, Tuple, Dict, Any  # Type hints (documentation)


# ============================================================================
# KVServer CLASS - The Database Server
# ============================================================================
class KVServer:
    """
    This is the SERVER - like a library that stores books (key-value pairs).
    
    HOW IT WORKS:
    1. Starts listening on a port (e.g., localhost:5555)
    2. Clients connect and send commands
    3. Server processes commands and responds
    4. All writes are saved to disk for durability
    """
    
    def __init__(self, host='localhost', port=5555, data_dir='./kvstore_data'):
        """
        Constructor - Initializes the server when you create it.
        
        Parameters:
        - host: Where server listens (localhost = this computer)
        - port: Port number (like door number, 5555 is default)
        - data_dir: Folder where data files are stored
        """
        # Store configuration
        self.host = host                    # Server address (localhost = 127.0.0.1)
        self.port = port                    # Port number (like apartment number)
        self.data_dir = Path(data_dir)      # Convert to Path object for easier file handling
        self.data_dir.mkdir(exist_ok=True)  # Create directory if it doesn't exist
        
        # Define file paths for persistence
        self.wal_path = self.data_dir / 'wal.log'           # Write-Ahead Log file
        self.snapshot_path = self.data_dir / 'snapshot.json'  # Snapshot file
        
        # In-memory storage (like a Python dictionary)
        # This is FAST but temporary - we also save to disk
        self.store: Dict[str, str] = {}
        
        # Thread lock - prevents multiple threads from modifying data simultaneously
        # RLock = Reentrant Lock (can be acquired multiple times by same thread)
        self.lock = threading.RLock()
        
        # Server state
        self.running = False           # Flag to control server loop
        self.server_socket = None       # TCP socket for accepting connections
        
        # Load any existing data from disk (if server was restarted)
        self._load_data()
    
    def _load_data(self):
        """
        Loads data from disk when server starts.
        
        PROCESS:
        1. Load snapshot.json (fast - contains all current data)
        2. Replay WAL (wal.log) - apply any operations after snapshot
        3. Result: Server has all data that was saved before shutdown
        
        WHY TWO FILES?
        - Snapshot: Fast to load, but expensive to create
        - WAL: Fast to write, but slow to replay if it's huge
        - Together: Best of both worlds!
        """
        # Step 1: Load snapshot if it exists
        if self.snapshot_path.exists():
            # Open snapshot file and load JSON data into memory
            with open(self.snapshot_path, 'r') as f:
                self.store = json.load(f)  # Load entire database state
        
        # Step 2: Replay WAL (Write-Ahead Log)
        # WAL contains operations that happened AFTER the snapshot
        if self.wal_path.exists():
            with open(self.wal_path, 'r') as f:
                # Read line by line (each line = one operation)
                for line in f:
                    if line.strip():  # Skip empty lines
                        # Parse JSON: {"op": "set", "key": "name", "value": "Ahmed"}
                        entry = json.loads(line)
                        # Apply the operation to restore state
                        self._apply_operation(entry)
    
    def _apply_operation(self, entry: Dict[str, Any]):
        """
        Applies a single operation from WAL to the in-memory store.
        
        This is used when:
        1. Replaying WAL on startup
        2. Processing client requests
        
        Parameters:
        - entry: Dictionary like {"op": "set", "key": "name", "value": "Ahmed"}
        """
        op = entry['op']  # Get operation type: "set", "delete", or "bulk_set"
        
        if op == 'set':
            # Set operation: store key-value pair
            self.store[entry['key']] = entry['value']
            
        elif op == 'delete':
            # Delete operation: remove key (pop returns None if key doesn't exist)
            self.store.pop(entry['key'], None)
            
        elif op == 'bulk_set':
            # Bulk set: set multiple key-value pairs at once
            for key, value in entry['items']:
                self.store[key] = value
    
    def _write_to_wal(self, entry: Dict[str, Any]):
        """
        Writes an operation to the Write-Ahead Log (WAL).
        
        WHY WAL?
        - Every write is IMMEDIATELY saved to disk
        - If server crashes, we can replay WAL to recover
        - This ensures 100% durability (no data loss)
        
        WHAT IS FSYNC?
        - f.flush() sends data to OS buffer (might not be on disk yet)
        - os.fsync() FORCES write to physical disk
        - Without fsync, data might be lost if power fails
        
        Parameters:
        - entry: Operation to log (e.g., {"op": "set", "key": "x", "value": "y"})
        """
        # Open WAL file in append mode (add to end of file)
        with open(self.wal_path, 'a') as f:
            # Convert operation to JSON string and write as one line
            f.write(json.dumps(entry) + '\n')
            # Force Python to send data to OS
            f.flush()
            # CRITICAL: Force OS to write to physical disk (not just RAM cache)
            os.fsync(f.fileno())  # fileno() gets file descriptor number
    
    def _create_snapshot(self):
        """
        Creates a snapshot (backup) of current database state.
        
        WHY SNAPSHOTS?
        - WAL grows forever (slow to replay)
        - Snapshot = complete copy of all data (fast to load)
        - After snapshot, we can truncate WAL
        
        PROCESS:
        1. Write snapshot to temporary file
        2. Atomically rename (prevents corruption if crash happens)
        3. Truncate WAL (clear it, since snapshot has all data)
        """
        # Create temporary file (safer - if crash happens, old snapshot stays intact)
        temp_path = self.snapshot_path.with_suffix('.tmp')
        
        # Write entire database to temporary file
        with open(temp_path, 'w') as f:
            json.dump(self.store, f)  # Write all key-value pairs as JSON
            f.flush()
            os.fsync(f.fileno())  # Force to disk
        
        # Atomic rename: rename is atomic operation (all-or-nothing)
        # This ensures snapshot is never corrupted (either old or new, never half-written)
        temp_path.replace(self.snapshot_path)
        
        # Truncate WAL (clear it) since snapshot has all data now
        with open(self.wal_path, 'w') as f:
            f.flush()
            os.fsync(f.fileno())
    
    def set(self, key: str, value: str) -> bool:
        """
        Sets a key-value pair in the store.
        
        PROCESS:
        1. Lock (prevent other threads from interfering)
        2. Write to WAL (save to disk FIRST - durability!)
        3. Update in-memory store (fast access)
        4. Return success
        
        WHY WRITE TO WAL FIRST?
        - If we crash after step 3 but before step 2, data is lost
        - Writing to WAL first ensures durability
        
        Parameters:
        - key: The key (like "name")
        - value: The value (like "Ahmed")
        Returns: True if successful
        """
        with self.lock:  # Acquire lock (only one thread can execute this block)
            # Create operation entry
            entry = {'op': 'set', 'key': key, 'value': value}
            # CRITICAL: Write to WAL FIRST (before updating memory)
            self._write_to_wal(entry)
            # Now update in-memory store (for fast reads)
            self.store[key] = value
            return True
    
    def get(self, key: str) -> Optional[str]:
        """
        Gets the value for a key.
        
        This is FAST because it only reads from memory (not disk).
        
        Parameters:
        - key: The key to look up
        Returns: The value, or None if key doesn't exist
        """
        with self.lock:  # Lock to prevent reading while writing
            # Return value from in-memory dictionary (None if not found)
            return self.store.get(key)
    
    def delete(self, key: str) -> bool:
        """
        Deletes a key from the store.
        
        PROCESS:
        1. Check if key exists
        2. Write delete operation to WAL
        3. Remove from memory
        
        Parameters:
        - key: The key to delete
        Returns: True if deleted, False if key didn't exist
        """
        with self.lock:
            if key in self.store:
                # Key exists - log deletion to WAL
                entry = {'op': 'delete', 'key': key}
                self._write_to_wal(entry)
                # Remove from memory
                del self.store[key]
                return True
            return False  # Key didn't exist
    
    def bulk_set(self, items: List[Tuple[str, str]]) -> bool:
        """
        Sets multiple key-value pairs at once (more efficient than many Set calls).
        
        Example: BulkSet([("k1", "v1"), ("k2", "v2"), ("k3", "v3")])
        
        Parameters:
        - items: List of (key, value) tuples
        Returns: True if successful
        """
        with self.lock:
            # Create bulk operation entry
            entry = {'op': 'bulk_set', 'items': items}
            # Write entire bulk operation to WAL (one write instead of many)
            self._write_to_wal(entry)
            # Update all keys in memory
            for key, value in items:
                self.store[key] = value
            return True
    
    def _handle_client(self, client_socket: socket.socket):
        """
        Handles communication with ONE client connection.
        
        This runs in a separate thread for each client (multiple clients can connect).
        
        PROTOCOL EXPLANATION:
        Our protocol uses length-prefixed messages:
        1. First 4 bytes: Length of message (big-endian integer)
        2. Next N bytes: JSON message (where N = length from step 1)
        
        WHY LENGTH-PREFIXED?
        - TCP is a stream (no message boundaries)
        - We need to know where one message ends and next begins
        - Length prefix tells us exactly how many bytes to read
        
        Parameters:
        - client_socket: The TCP socket connected to a client
        """
        try:
            # Keep handling requests until client disconnects or server stops
            while self.running:
                # STEP 1: Receive message length (4 bytes)
                length_bytes = client_socket.recv(4)  # Read exactly 4 bytes
                if not length_bytes:  # Client disconnected (empty = connection closed)
                    break
                
                # Convert 4 bytes to integer (big-endian = most significant byte first)
                msg_length = int.from_bytes(length_bytes, 'big')
                
                # STEP 2: Receive the actual message (read exactly msg_length bytes)
                data = b''  # Empty bytes object to accumulate data
                while len(data) < msg_length:
                    # Read in chunks (max 4096 bytes at a time)
                    # This handles large messages efficiently
                    chunk = client_socket.recv(min(4096, msg_length - len(data)))
                    if not chunk:  # Connection closed unexpectedly
                        break
                    data += chunk  # Append chunk to accumulated data
                
                if not data:  # No data received
                    break
                
                # STEP 3: Parse JSON message
                request = json.loads(data.decode('utf-8'))
                # Example: {"cmd": "set", "key": "name", "value": "Ahmed"}
                
                # STEP 4: Process the request
                response = self._process_request(request)
                
                # STEP 5: Send response back to client
                response_data = json.dumps(response).encode('utf-8')  # Convert to bytes
                response_length = len(response_data).to_bytes(4, 'big')  # Length as 4 bytes
                # Send: [4 bytes length][JSON response]
                client_socket.sendall(response_length + response_data)
        
        except Exception as e:
            # Handle any errors gracefully
            print(f"Error handling client: {e}")
        finally:
            # Always close the connection when done
            client_socket.close()
    
    def _process_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Processes a client request and returns a response.
        
        This is like a router - it looks at the command and calls the right function.
        
        Parameters:
        - request: Dictionary like {"cmd": "set", "key": "x", "value": "y"}
        Returns: Dictionary like {"status": "ok", "result": True}
        """
        cmd = request.get('cmd')  # Get command type
        
        if cmd == 'set':
            # Set command: store key-value pair
            success = self.set(request['key'], request['value'])
            return {'status': 'ok', 'result': success}
        
        elif cmd == 'get':
            # Get command: retrieve value
            value = self.get(request['key'])
            return {'status': 'ok', 'result': value}
        
        elif cmd == 'delete':
            # Delete command: remove key
            success = self.delete(request['key'])
            return {'status': 'ok', 'result': success}
        
        elif cmd == 'bulk_set':
            # Bulk set command: set multiple pairs
            success = self.bulk_set(request['items'])
            return {'status': 'ok', 'result': success}
        
        else:
            # Unknown command
            return {'status': 'error', 'message': 'Unknown command'}
    
    def start(self):
        """
        Starts the server - begins listening for client connections.
        
        PROCESS:
        1. Create TCP socket
        2. Bind to host:port
        3. Start listening
        4. Start snapshot thread (periodic backups)
        5. Accept client connections (one thread per client)
        """
        self.running = True  # Set flag to True
        
        # Create TCP socket
        # AF_INET = IPv4, SOCK_STREAM = TCP (reliable, ordered)
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        
        # Allow reusing address (if server crashes, can restart immediately)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        
        # Bind socket to host and port (like opening a door)
        self.server_socket.bind((self.host, self.port))
        
        # Start listening (queue up to 5 pending connections)
        self.server_socket.listen(5)
        
        print(f"Server started on {self.host}:{self.port}")
        
        # Start snapshot worker thread (runs in background)
        def snapshot_worker():
            """
            Background thread that creates snapshots periodically.
            
            WHY BACKGROUND THREAD?
            - Doesn't block main server loop
            - Server can still handle clients while snapshotting
            """
            while self.running:
                time.sleep(10)  # Wait 10 seconds
                if self.running:
                    with self.lock:
                        # Only create snapshot if WAL is large enough (>10KB)
                        # This prevents creating snapshots too frequently
                        if os.path.exists(self.wal_path) and os.path.getsize(self.wal_path) > 10000:
                            self._create_snapshot()
        
        # Start snapshot thread as daemon (dies when main program exits)
        threading.Thread(target=snapshot_worker, daemon=True).start()
        
        # Main server loop - accept client connections
        while self.running:
            try:
                # Set timeout so we can check self.running periodically
                self.server_socket.settimeout(1.0)
                try:
                    # Accept incoming connection (blocks until client connects)
                    client_socket, addr = self.server_socket.accept()
                    # addr = (IP address, port) of client
                    
                    # Handle client in separate thread (allows multiple clients)
                    # daemon=True means thread dies when main program exits
                    threading.Thread(target=self._handle_client, args=(client_socket,), daemon=True).start()
                except socket.timeout:
                    # Timeout is normal - just check self.running and continue
                    continue
            except Exception as e:
                # Handle any other errors
                if self.running:
                    print(f"Server error: {e}")
    
    def stop(self):
        """
        Stops the server gracefully.
        
        GRACEFUL SHUTDOWN:
        1. Set running flag to False (stops accepting new connections)
        2. Create final snapshot (save all data)
        3. Close server socket
        
        This ensures no data is lost when shutting down.
        """
        print("Stopping server...")
        self.running = False  # Stop accepting new connections
        
        # Create final snapshot before shutdown (save everything)
        with self.lock:
            self._create_snapshot()
        
        # Close server socket
        if self.server_socket:
            self.server_socket.close()
        
        print("Server stopped")


# ============================================================================
# KVClient CLASS - The Client Library
# ============================================================================
class KVClient:
    """
    This is the CLIENT - what applications use to talk to the server.
    
    HOW IT WORKS:
    1. Connects to server (like opening a phone line)
    2. Sends commands (Set/Get/Delete/BulkSet)
    3. Receives responses
    4. Handles all the networking details for you
    
    USAGE:
    client = KVClient(port=5555)
    client.Set("name", "Ahmed")
    value = client.Get("name")
    client.Delete("name")
    client.close()
    """
    
    def __init__(self, host='localhost', port=5555):
        """
        Creates a client and connects to the server.
        
        Parameters:
        - host: Server address (default: localhost)
        - port: Server port (default: 5555)
        """
        self.host = host
        self.port = port
        self.socket = None  # Will hold TCP socket
        self._connect()  # Connect immediately
    
    def _connect(self):
        """
        Establishes TCP connection to the server.
        
        This is like dialing a phone number - you need to connect before talking.
        """
        # Create TCP socket
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # Connect to server (like dialing the phone)
        self.socket.connect((self.host, self.port))
    
    def _send_request(self, request: Dict[str, Any]) -> Dict[str, Any]:
        """
        Sends a request to the server and receives the response.
        
        This handles all the protocol details (length prefix, JSON encoding).
        
        PROCESS:
        1. Convert request to JSON bytes
        2. Send: [4 bytes length][JSON data]
        3. Receive: [4 bytes length][JSON response]
        4. Parse and return response
        
        Parameters:
        - request: Dictionary like {"cmd": "set", "key": "x", "value": "y"}
        Returns: Dictionary like {"status": "ok", "result": True}
        """
        # STEP 1: Convert request to JSON string, then to bytes
        request_data = json.dumps(request).encode('utf-8')
        # STEP 2: Calculate length and convert to 4 bytes (big-endian)
        request_length = len(request_data).to_bytes(4, 'big')
        
        # STEP 3: Send length prefix + data (all at once)
        self.socket.sendall(request_length + request_data)
        
        # STEP 4: Receive response length (4 bytes)
        length_bytes = self.socket.recv(4)
        response_length = int.from_bytes(length_bytes, 'big')
        
        # STEP 5: Receive full response (read exactly response_length bytes)
        data = b''
        while len(data) < response_length:
            # Read in chunks (handles large responses)
            chunk = self.socket.recv(min(4096, response_length - len(data)))
            if not chunk:
                raise ConnectionError("Connection closed")
            data += chunk
        
        # STEP 6: Parse JSON response and return
        return json.loads(data.decode('utf-8'))
    
    def Set(self, key: str, value: str) -> bool:
        """
        Sets a key-value pair on the server.
        
        This is what you call from your application code.
        
        Parameters:
        - key: The key to set
        - value: The value to store
        Returns: True if successful
        """
        # Send set command to server
        response = self._send_request({'cmd': 'set', 'key': key, 'value': value})
        return response['result']  # Extract result from response
    
    def Get(self, key: str) -> Optional[str]:
        """
        Gets the value for a key from the server.
        
        Parameters:
        - key: The key to look up
        Returns: The value, or None if key doesn't exist
        """
        # Send get command to server
        response = self._send_request({'cmd': 'get', 'key': key})
        return response['result']  # Extract value from response
    
    def Delete(self, key: str) -> bool:
        """
        Deletes a key from the server.
        
        Parameters:
        - key: The key to delete
        Returns: True if deleted, False if key didn't exist
        """
        # Send delete command to server
        response = self._send_request({'cmd': 'delete', 'key': key})
        return response['result']
    
    def BulkSet(self, items: List[Tuple[str, str]]) -> bool:
        """
        Sets multiple key-value pairs at once (more efficient).
        
        Parameters:
        - items: List of (key, value) tuples
        Returns: True if successful
        
        Example:
        client.BulkSet([("k1", "v1"), ("k2", "v2")])
        """
        # Send bulk_set command to server
        response = self._send_request({'cmd': 'bulk_set', 'items': items})
        return response['result']
    
    def close(self):
        """
        Closes the connection to the server.
        
        Always call this when done to free resources.
        """
        if self.socket:
            self.socket.close()


# ============================================================================
# TESTS - Verify Everything Works Correctly
# ============================================================================
def run_tests():
    """
    Runs comprehensive tests to verify the key-value store works correctly.
    
    TESTS COVER:
    1. Set then Get - Basic storage and retrieval
    2. Set then Delete then Get - Deletion works
    3. Get without setting - Returns None for missing keys
    4. Set then Set (same key) - Updates work (overwrites old value)
    5. Set then exit then Get - Persistence works (data survives restart)
    6. BulkSet - Bulk operations work
    
    All tests use the KVClient (as required by the task).
    """
    import shutil
    
    # Clean up old test data (start fresh)
    data_dir = './test_kvstore_data'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)  # Delete entire directory
    
    # Start server on port 5556 (different from default to avoid conflicts)
    server = KVServer(port=5556, data_dir=data_dir)
    # Start server in background thread (non-blocking)
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(1)  # Wait for server to start listening
    
    try:
        print("Running tests...")
        
        # ====================================================================
        # TEST 1: Set then Get
        # ====================================================================
        print("\nTest 1: Set then Get")
        client = KVClient(port=5556)  # Connect to server
        client.Set('key1', 'value1')  # Store data
        result = client.Get('key1')   # Retrieve data
        # Verify: result should be 'value1'
        assert result == 'value1', f"Expected 'value1', got {result}"
        print("✓ Passed")
        
        # ====================================================================
        # TEST 2: Set then Delete then Get
        # ====================================================================
        print("\nTest 2: Set then Delete then Get")
        client.Set('key2', 'value2')  # Store
        client.Delete('key2')          # Delete
        result = client.Get('key2')    # Try to get (should be None)
        assert result is None, f"Expected None, got {result}"
        print("✓ Passed")
        
        # ====================================================================
        # TEST 3: Get without setting
        # ====================================================================
        print("\nTest 3: Get without setting")
        result = client.Get('nonexistent')  # Get key that doesn't exist
        assert result is None, f"Expected None, got {result}"
        print("✓ Passed")
        
        # ====================================================================
        # TEST 4: Set then Set (same key) then Get
        # ====================================================================
        print("\nTest 4: Set then Set (same key) then Get")
        client.Set('key3', 'value3')           # First set
        client.Set('key3', 'value3_updated')    # Overwrite (same key)
        result = client.Get('key3')
        # Should get the UPDATED value, not the old one
        assert result == 'value3_updated', f"Expected 'value3_updated', got {result}"
        print("✓ Passed")
        
        # ====================================================================
        # TEST 5: Set then exit (gracefully) then Get
        # ====================================================================
        print("\nTest 5: Set then exit (gracefully) then Get")
        client.Set('key4', 'value4')  # Store data
        client.close()                # Close client connection
        
        # Stop server gracefully (saves snapshot)
        server.stop()
        time.sleep(2)  # Wait for snapshot to complete
        
        # Restart server (should load data from disk)
        server = KVServer(port=5556, data_dir=data_dir)
        server_thread = threading.Thread(target=server.start, daemon=True)
        server_thread.start()
        time.sleep(1)  # Wait for server to start
        
        # Reconnect and verify data survived restart
        client = KVClient(port=5556)
        result = client.Get('key4')
        # This proves persistence works - data survived server restart!
        assert result == 'value4', f"Expected 'value4', got {result}"
        print("✓ Passed")
        
        # ====================================================================
        # TEST 6: BulkSet
        # ====================================================================
        print("\nTest 6: BulkSet")
        # Create 100 key-value pairs
        items = [(f'bulk_{i}', f'value_{i}') for i in range(100)]
        client.BulkSet(items)  # Set all at once
        result = client.Get('bulk_50')  # Verify one of them
        assert result == 'value_50', f"Expected 'value_50', got {result}"
        print("✓ Passed")
        
        client.close()
        print("\n✓ All tests passed!")
        
    finally:
        # Always stop server, even if test fails
        server.stop()
        time.sleep(1)


# ============================================================================
# BENCHMARKS - Measure Performance
# ============================================================================
def run_benchmarks():
    """
    Runs performance benchmarks to measure:
    1. WRITE THROUGHPUT: How many writes per second
    2. DURABILITY: What percentage of writes survive crashes
    
    BENCHMARK 1: Write Throughput
    ------------------------------
    Tests how write performance changes as database grows.
    - Pre-populates database with different sizes (0, 1K, 10K, 50K keys)
    - Measures time to write 10,000 new keys
    - Calculates: writes/second and latency (ms per write)
    
    BENCHMARK 2: Durability Test
    -----------------------------
    Tests if data survives crashes (100% durability goal).
    - Writer thread: Continuously writes data, tracks acknowledged writes
    - Killer thread: Randomly kills and restarts server
    - After test: Checks which acknowledged writes survived
    - Result: Durability percentage (should be 100%)
    """
    import shutil
    
    # Clean up old benchmark data
    data_dir = './bench_kvstore_data'
    if os.path.exists(data_dir):
        shutil.rmtree(data_dir)
    
    # Start server on port 5557
    server = KVServer(port=5557, data_dir=data_dir)
    server_thread = threading.Thread(target=server.start, daemon=True)
    server_thread.start()
    time.sleep(1)
    
    try:
        print("\n" + "="*60)
        print("BENCHMARKS")
        print("="*60)
        
        client = KVClient(port=5557)
        
        # ====================================================================
        # BENCHMARK 1: Write Throughput
        # ====================================================================
        print("\nBenchmark 1: Write Throughput")
        print("  Testing how write speed changes with database size...")
        
        # Test with different database sizes
        for data_size in [0, 1000, 10000, 50000]:
            # Pre-populate database (if data_size > 0)
            if data_size > 0:
                # Create list of key-value pairs
                items = [(f'prepop_{i}', f'value_{i}') for i in range(data_size)]
                # Insert in batches of 1000 (more efficient)
                for i in range(0, len(items), 1000):
                    client.BulkSet(items[i:i+1000])
            
            # Now measure write throughput
            num_writes = 10000  # Write 10,000 keys
            start_time = time.time()  # Record start time
            
            # Perform writes
            for i in range(num_writes):
                client.Set(f'bench_key_{i}', f'bench_value_{i}')
            
            elapsed = time.time() - start_time  # Calculate elapsed time
            throughput = num_writes / elapsed    # Writes per second
            latency = (elapsed * 1000) / num_writes  # Milliseconds per write
            
            # Print results
            print(f"  Data size: {data_size:6d} | Throughput: {throughput:8.2f} writes/sec | Latency: {latency:.3f} ms/write")
        
        # ====================================================================
        # BENCHMARK 2: Durability Test
        # ====================================================================
        print("\nBenchmark 2: Durability Test")
        print("  Testing write acknowledgment vs actual persistence...")
        print("  (Writer thread writes data, killer thread crashes server)")
        
        # Shared data structures
        acknowledged_keys = set()  # Keys we received ACK for (should survive)
        lost_keys = set()          # Keys that were lost after crash
        lock = threading.Lock()    # Lock for thread-safe access
        stop_flag = threading.Event()  # Signal to stop threads
        
        def writer():
            """
            Writer thread: Continuously writes data to server.
            
            PROCESS:
            1. Connect to server
            2. Write keys: durable_0, durable_1, durable_2, ...
            3. After each write, add key to acknowledged_keys
            4. If connection lost (server crashed), reconnect and continue
            """
            client_w = None  # Client connection (will be created)
            i = 0            # Counter for keys
            
            while not stop_flag.is_set():  # Keep writing until told to stop
                try:
                    # Reconnect if needed (first time or after crash)
                    if client_w is None:
                        try:
                            client_w = KVClient(port=5557)  # Connect to server
                        except Exception:
                            # Server not ready yet, wait and retry
                            time.sleep(0.1)
                            continue
                    
                    # Write a key-value pair
                    key = f'durable_{i}'
                    client_w.Set(key, f'value_{i}')
                    
                    # Record that we got acknowledgment for this key
                    with lock:
                        acknowledged_keys.add(key)
                    
                    i += 1
                    time.sleep(0.001)  # Small delay (1ms) between writes
                    
                except (ConnectionError, ConnectionAbortedError, OSError):
                    # Connection lost (server crashed or restarted)
                    # Close old connection
                    if client_w:
                        try:
                            client_w.close()
                        except:
                            pass
                    client_w = None  # Will reconnect on next iteration
                    time.sleep(0.1)  # Brief pause before reconnecting
            
            # Clean up: close connection when done
            if client_w:
                try:
                    client_w.close()
                except:
                    pass
        
        def killer():
            """
            Killer thread: Randomly kills and restarts the server.
            
            This simulates crashes and tests if data survives.
            
            PROCESS:
            1. Wait 2 seconds (let some data accumulate)
            2. Kill server 3 times (stop it)
            3. After each kill, restart server
            4. Server should load data from disk on restart
            """
            nonlocal server, server_thread
            
            time.sleep(2)  # Let writer accumulate some data first
            
            # Kill and restart 3 times
            for _ in range(3):
                time.sleep(1)  # Wait 1 second between kills
                print("    Killing server...")
                server.stop()  # Stop server (saves snapshot)
                time.sleep(0.5)  # Brief pause
                
                # Restart server (loads data from disk)
                print("    Restarting server...")
                server = KVServer(port=5557, data_dir=data_dir)
                server_thread = threading.Thread(target=server.start, daemon=True)
                server_thread.start()
                time.sleep(1)  # Wait for server to start
        
        # Start both threads
        writer_thread = threading.Thread(target=writer, daemon=True)
        killer_thread = threading.Thread(target=killer, daemon=True)
        
        writer_thread.start()  # Start writing
        killer_thread.start()  # Start killing
        
        # Wait for killer thread to finish (3 kills)
        killer_thread.join()
        stop_flag.set()  # Tell writer to stop
        writer_thread.join()  # Wait for writer to finish
        time.sleep(1)  # Brief pause
        
        # ====================================================================
        # CHECK RESULTS: Which keys survived?
        # ====================================================================
        client_check = KVClient(port=5557)
        
        # Check each acknowledged key - is it still in the database?
        with lock:
            for key in acknowledged_keys:
                if client_check.Get(key) is None:
                    # Key was lost! (shouldn't happen with 100% durability)
                    lost_keys.add(key)
        
        # Calculate durability percentage
        # Formula: (1 - lost/total) * 100
        # Example: 1000 total, 0 lost = 100% durability
        durability = (1 - len(lost_keys) / len(acknowledged_keys)) * 100 if acknowledged_keys else 100
        
        # Print results
        print(f"  Total acknowledged writes: {len(acknowledged_keys)}")
        print(f"  Lost writes: {len(lost_keys)}")
        print(f"  Durability: {durability:.2f}%")
        
        # With proper WAL + fsync, this should be 100%!
        
        client.close()
        client_check.close()
        
    finally:
        server.stop()
        time.sleep(1)


# ============================================================================
# MAIN ENTRY POINT
# ============================================================================
if __name__ == '__main__':
    """
    When you run this file directly (python kv_store.py), it will:
    1. Run all tests (verify correctness)
    2. Run all benchmarks (measure performance)
    
    This is useful for development and verification.
    """
    run_tests()      # Run correctness tests first
    run_benchmarks() # Then run performance benchmarks
