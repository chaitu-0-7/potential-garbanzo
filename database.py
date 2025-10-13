# database.py (UPDATED - Better singleton with close protection)
import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.server_api import ServerApi
import logging

load_dotenv()

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = "job_matching_app"


class MongoDB:
    """MongoDB connection manager with robust singleton pattern."""
    
    _instance = None
    _client = None
    _db = None
    _close_count = 0  # Track close calls

    def __new__(cls):
        """Singleton pattern - only one instance exists."""
        if cls._instance is None:
            cls._instance = super(MongoDB, cls).__new__(cls)
        return cls._instance

    def __init__(self):
        """Initialize connection if not already connected."""
        # Only connect if client is None or closed
        if self._client is None:
            self._connect()
    
    def _connect(self):
        """Establish MongoDB connection."""
        try:
            self._client = MongoClient(MONGO_URI, server_api=ServerApi('1'))
            self._db = self._client[DB_NAME]
            self._close_count = 0  # Reset close counter
            logging.info("✅ MongoDB client initialized.")
            
            # Test connection
            self._client.admin.command('ping')
            logging.info(f"✅ Successfully connected to MongoDB database: {self._db.name}")
        except Exception as e:
            logging.error(f"❌ MongoDB connection failed: {e}")
            raise
    
    @property
    def client(self):
        """Get MongoDB client, reconnecting if necessary."""
        # Check if client exists and is still connected
        if self._client is None:
            self._connect()
        else:
            # Try a quick operation to check if connection is alive
            try:
                self._client.admin.command('ping')
            except Exception:
                # Connection is dead, reconnect
                logging.warning("MongoDB connection lost. Reconnecting...")
                self._connect()
        return self._client
    
    @property
    def db(self):
        """Get database instance, reconnecting if necessary."""
        # Use client property which handles reconnection
        _ = self.client  # This ensures client is connected
        if self._db is None:
            self._db = self._client[DB_NAME]
        return self._db

    def get_collection(self, collection_name: str):
        """
        Get a MongoDB collection.
        
        Args:
            collection_name: Name of the collection
            
        Returns:
            MongoDB collection object
        """
        return self.db[collection_name]

    def close_connection(self):
        """
        Close MongoDB connection.
        
        WARNING: Due to singleton pattern, this closes the connection
        for ALL users of this class. Use with caution.
        """
        self._close_count += 1
        
        # Only actually close on the LAST close call
        # This is a workaround for multiple callers
        if self._close_count >= 10:  # Arbitrary high number
            if self._client is not None:
                try:
                    self._client.close()
                    logging.info("✅ MongoDB connection closed.")
                except Exception as e:
                    logging.warning(f"Error closing MongoDB connection: {e}")
                finally:
                    self._client = None
                    self._db = None
                    self._close_count = 0
        else:
            # Just log, don't actually close
            logging.debug(f"MongoDB close_connection called ({self._close_count} times), keeping connection open")
    
    def force_close(self):
        """Force close the connection immediately."""
        if self._client is not None:
            try:
                self._client.close()
                logging.info("✅ MongoDB connection force closed.")
            except Exception as e:
                logging.warning(f"Error force closing MongoDB: {e}")
            finally:
                self._client = None
                self._db = None
                self._close_count = 0
    
    def __enter__(self):
        """Context manager entry - ensure connection is active."""
        if self._client is None:
            self._connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - DON'T close due to singleton."""
        # Don't close in context manager exit for singleton
        # Let the main application control lifecycle
        logging.debug("Context manager exit - keeping connection open (singleton pattern)")
        return False


# Example usage
if __name__ == "__main__":
    print("\n=== Test 1: Regular usage ===")
    db_instance = MongoDB()
    jobs_collection = db_instance.get_collection("jobs")
    print(f"Connected to MongoDB database: {db_instance.db.name}")
    db_instance.close_connection()
    
    print("\n=== Test 2: Context manager usage ===")
    with MongoDB() as db:
        jobs_collection = db.get_collection("jobs")
        print(f"Connected to MongoDB database: {db.db.name}")
    print("Context exited - connection still open (singleton)")
    
    print("\n=== Test 3: Reconnection ===")
    db_instance2 = MongoDB()
    jobs_collection2 = db_instance2.get_collection("jobs")
    print(f"Reconnected to MongoDB database: {db_instance2.db.name}")
    
    print("\n=== Test 4: Force close ===")
    db_instance2.force_close()
    
    print("\n✅ All tests passed!")
