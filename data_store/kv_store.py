import json
import threading
import time
import os

class KeyValueStore:
    MAX_FILE_SIZE = 1 * 1024 * 1024 * 1024  # 1GB in bytes
    MAX_ITEMS_IN_MEMORY = 1000  # Arbitrary limit for memory efficiency

    def __init__(self, file_path=None):
        """Initializes the Key-Value Store."""
        self.file_path = file_path or "default_kv_store.json"
        self.lock = threading.Lock()
        self.store = {}
        
        # Load data if the file exists
        if os.path.exists(self.file_path):
            self.load_data()

    def load_data(self):
        """Load existing data from the file into the in-memory store."""
        try:
            with open(self.file_path, 'r') as file:
                self.store = json.load(file)
                self.store = {key: (value['value'], value['expiry']) for key, value in self.store.items()}
            print("Data loaded successfully from file.")
        except json.JSONDecodeError:
            print("File exists but is empty or corrupted. Starting with an empty store.")
            self.store = {}
        except FileNotFoundError:
            print("File not found. Starting with an empty store.")
            self.store = {}

    def save_data(self):
        """Save the in-memory store data to the file."""
        # Check file size before saving
        if os.path.exists(self.file_path) and os.path.getsize(self.file_path) >= self.MAX_FILE_SIZE:
            raise Exception("Cannot save data: file size limit of 1GB exceeded.")

        try:
            data_to_save = {key: {'value': value[0], 'expiry': value[1]} for key, value in self.store.items()}
            with open(self.file_path, 'w') as file:
                json.dump(data_to_save, file)
            print("Data saved successfully to file.")
        except Exception as e:
            print(f"An error occurred while saving data: {e}")

    def add(self, key, value, ttl=None):
        """Add a new key-value pair to the store with an optional TTL."""
        with self.lock:
            if len(key) > 32:
                raise ValueError("Key must not exceed 32 characters.")
            if len(value) > 16 * 1024:
                raise ValueError("Value must not exceed 16KB.")
            if key in self.store:
                raise KeyError(f"Key '{key}' already exists.")

            # Validate TTL
            if ttl is not None:
                if not isinstance(ttl, (int, float)):
                    raise ValueError("TTL must be a number.")
                expiration_time = time.time() + ttl
            else:
                expiration_time = None

            # Save key and expiration time
            self.store[key] = (value, expiration_time)
            print(f"Added key: {key}, expiration time: {expiration_time}")

            # Save data after addition
            self.save_data()

    def retrieve(self, key):
        """Retrieve the value associated with a given key."""
        with self.lock:
            if key not in self.store:
                raise KeyError(f"Key '{key}' not found.")
            
            value, expiry = self.store[key]
            current_time = time.time()

            print(f"Retrieving key: {key}, expiry: {expiry}")

            if expiry is not None:
                if not isinstance(expiry, (int, float)):  # Ensure expiry is a number
                    raise ValueError("Invalid expiry time.")
                if current_time > expiry:
                    del self.store[key]
                    self.save_data()
                    raise KeyError(f"Key '{key}' has expired.")

            return value

    def remove(self, key):
        """Remove a key-value pair using the specified key."""
        with self.lock:
            if key not in self.store:
                raise KeyError(f"Key '{key}' not found.")
            
            del self.store[key]
            self.save_data()

    def batch_add_with_ttl(self, items_with_ttl):
        """Add multiple key-value pairs to the store with optional TTL."""
        with self.lock:
            if len(items_with_ttl) > 100:
                raise ValueError("Batch size exceeds the maximum limit of 100 items.")
            for key, (value, ttl) in items_with_ttl.items():
                if len(key) > 32:
                    raise ValueError(f"Key '{key}' must not exceed 32 characters.")
                if len(value) > 16 * 1024:
                    raise ValueError(f"Value for key '{key}' must not exceed 16KB.")
            
                expiration_time = time.time() + ttl if ttl is not None else None
                self.store[key] = (value, expiration_time)
                print(f"Batch added key: {key}, expiration time: {expiration_time}")

            self.save_data()
            print("Batch added successfully.")


# Example usage and error handling
if __name__ == "__main__":
    kv_store = KeyValueStore()

    try:
        kv_store.add("example_key", {"name": "Test"}, ttl=10)
        print("Added example_key.")
    except Exception as e:
        print(f"Error: {e}")

    try:
        value = kv_store.retrieve("example_key")
        print("Retrieved value:", value)
    except KeyError as ke:
        print(f"KeyError: {ke}")

    try:
        kv_store.remove("example_key")
        print("Removed example_key.")
    except KeyError as ke:
        print(f"KeyError: {ke}")

    # Example of batch operation
    try:
        kv_store.batch_add({
            "key1": {"name": "Value1"},
            "key2": {"name": "Value2"},
            "key4": {"name": "Value4"},
        })
    except Exception as e:
        print(f"Error: {e}")
