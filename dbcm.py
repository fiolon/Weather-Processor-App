import sqlite3

class DBCM:
  def __init__(self, db_name):
    """
    Initializes database
    """
    self.db_name = db_name
    self.connection = None
    self.cursor = None

  def __enter__(self):
    """
    Connects to database
    """
    self.connection = sqlite3.connect(self.db_name)
    self.cursor = self.connection.cursor()
    return self.cursor

  def __exit__(self, exc_type, exc_val, exc_tb):
    """
    Exits connection
    """
    if exc_type is None:
      self.connection.commit()
    self.cursor.close()
    self.connection.close()