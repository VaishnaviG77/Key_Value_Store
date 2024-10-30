import unittest
import os
import json
from data_store.kv_store import KeyValueStore

class TestKeyValueStore(unittest.TestCase):
    
    def setUp(self):
        """Set up a temporary file path for testing and initialize the KeyValueStore."""
        self.test_file = "test_kv_store.json"
        self.kv_store = KeyValueStore(file_path=self.test_file)
    
    def tearDown(self):
        """Remove the test file after each test to avoid interference with other tests."""
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
    
    def test_load_data_from_nonexistent_file(self):
        """Test that loading from a non-existent file starts with an empty store."""
        # Remove the file if it somehow exists from previous runs
        if os.path.exists(self.test_file):
            os.remove(self.test_file)
        # Create a new KeyValueStore instance to simulate loading
        kv_store = KeyValueStore(file_path=self.test_file)
        self.assertEqual(kv_store.store, {}, "Store should be empty when loading from a non-existent file.")

    def test_save_data(self):
        """Test that data is saved correctly to the file."""
        # Add sample data to the store
        self.kv_store.store["test_key"] = {"name": "Test", "value": 123}
        
        # Save the data to the file
        self.kv_store.save_data()
        
        # Check if data is correctly saved in the file
        with open(self.test_file, 'r') as file:
            data = json.load(file)
        self.assertEqual(data, {"test_key": {"name": "Test", "value": 123}}, "Data in file does not match the expected content.")
    
    def test_load_data(self):
        """Test loading data from an existing file."""
        # Manually create a JSON file with test data
        sample_data = {"test_key": {"name": "Test", "value": 123}}
        with open(self.test_file, 'w') as file:
            json.dump(sample_data, file)
        
        # Reload the data using KeyValueStore
        kv_store = KeyValueStore(file_path=self.test_file)
        
        self.assertEqual(kv_store.store, sample_data, "Loaded data does not match the expected content.")

if __name__ == "__main__":
    unittest.main()
