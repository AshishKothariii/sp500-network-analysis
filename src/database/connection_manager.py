import sqlite3

class ConnectionManager:
    _db_path = "e:\\1_practice\\financial-analysis-platform\\examples\\stock_data.db"
    _instance = None
    _connection = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(ConnectionManager, cls).__new__(cls)
        return cls._instance
    
    @classmethod
    def get_connection(cls):
        if cls._connection is None:
            cls._connection = sqlite3.connect(cls._db_path)
            print("New database connection created")
        else:
            print("Returning existing database connection")
        return cls._connection
    
    @classmethod 
    def set_db_path(cls, db_path):
        cls._db_path = db_path
    
    @classmethod
    def get_db_path(cls):
        return cls._db_path
    
    @classmethod
    def close(cls):
        if cls._connection is not None:
            cls._connection.close()
            cls._connection = None
            print("Database connection closed")
        else:
            print("No active database connection to close")
    
    @classmethod
    def is_connected(cls):
        return cls._connection is not None