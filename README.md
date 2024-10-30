# Key-Value Data Store

## Overview
A simple key-value data store that supports Create, Read, and Delete (CRD) operations, designed for managing student data efficiently.

## Features
- Local file-based storage
- Time-to-Live (TTL) for keys
- Batch operations for adding multiple entries
- Thread-safe for concurrent access
- Memory-efficient design

## Setup Instructions
1. Clone the repository:
   ```bash
   git clone [your-repo-url]

    Navigate to the project directory:

    bash

cd key-value-data-store

Install any required dependencies (if applicable).
Run the application:

bash

    python main.py

Testing

To run tests, execute:

bash

python -m unittest tests/test_kv_store.py

Design Decisions

    Data Structure: Chose a dictionary for in-memory storage to allow fast key lookups.
    File Size Management: Implemented checks to ensure the data file does not exceed 1GB.
    Batch Operations: Limited to 100 items for efficient memory usage and performance.

Dependencies

    Python 3.x
    Any other libraries (list them here if necessary)

OS Compatibility

The data store is designed to work on Windows, Linux, and macOS. If you encounter OS-specific issues, please note them here.
Time Spent

Approximately 3 hours.
About Me

Include a brief introduction about yourself or a link to your CV/profile.
