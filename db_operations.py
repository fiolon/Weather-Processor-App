from dbcm import DBCM
import sqlite3
import os

class DBOperations:
  def __init__(self, db_name="weather_data.db"):
    self.db_name = os.path.join(os.path.expanduser("~"), "weather_data.db")

  def initialize_db(self):
    """
    Initializes the database with the schema if it doesn't exit yet.
    """
    with DBCM(self.db_name) as cursor:
      cursor.execute("""
          CREATE TABLE IF NOT EXISTS weather (
                     id INTEGER PRIMARY KEY AUTOINCREMENT,
                     sample_date TEXT NOT NULL,
                     location TEXT NOT NULL,
                     min_temp REAL,
                     max_temp REAL,
                     avg_temp REAL,
                     UNIQUE(sample_date, location)
          )
      """)
      print("Database created.")

  def save_data(self, weather_data, location="Winnipeg"):
    """
    Saves weather data to the database.
    """
    with DBCM(self.db_name) as cursor:
      for date, temps in weather_data.items():
        try:
          cursor.execute("""
              INSERT OR IGNORE INTO weather (sample_date, location, min_temp, max_temp, avg_temp)
              VALUES (?, ?, ?, ?, ?)
          """, (date, location, temps["Min"], temps["Max"], temps["Mean"]))
        except sqlite3.IntegrityError as e:
          print(f"Skipping duplicate entry for {date} in {location}: {e}")

    print("Weather data saved in database.")

  def fetch_data(self, start_date=None, end_date=None, location="Winnipeg"):
    """
    Fetches weather data within a date range and location.
    """
    with DBCM(self.db_name) as cursor:
      query = """
          SELECT sample_date, min_temp, max_temp, avg_temp
          FROM weather
          WHERE location = ?
      """
      params = [location]

      if start_date and end_date:
        query += " AND sample_date BETWEEN ? AND ?"
        params.extend([start_date, end_date])

      cursor.execute(query, tuple(params))
      return cursor.fetchall()

  def purge_data(self):
    """
    Deletes all data from weather table without remove database.
    """
    with DBCM(self.db_name) as cursor:
      cursor.execute("DELETE FROM weather")
    print("All weather data purged from database.")

# Run DBOperations
if __name__ == "__main__":
  from scrape_weather import WeatherScraper

  # Call DBOperation
  db_ops = DBOperations()
  # Initialize database
  db_ops.initialize_db()

  # Scrapes data from URL
  base_url = "http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
  scraper = WeatherScraper(base_url)
  weather_data = scraper.scrape_weather_data()

  db_ops.save_data(weather_data, location="Winnipeg")

  # Fetches data between start date and end date
  rows = db_ops.fetch_data(start_date="2024-01-01", end_date="2024-12-12")
  print("Fetched data:", rows)

  db_ops.purge_data() # Deletes data but not database
