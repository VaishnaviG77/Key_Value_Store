from data_store.kv_store import KeyValueStore

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
        kv_store.batch_add_with_ttl({
            "key1": ({"name": "Value1"}, 10),  # 10 seconds TTL
            "key2": ({"name": "Value2"}, 20),  # 20 seconds TTL
            "key3": ({"name": "Value3"}, None),  # No TTL
        })
    except Exception as e:
        print(f"Error: {e}")

    # Confirming retrieval of batch added keys
    for key in ["key1", "key2", "key3"]:
        try:
            value = kv_store.retrieve(key)
            print(f"Retrieved {key}: {value}")
        except KeyError as e:
            print(e)