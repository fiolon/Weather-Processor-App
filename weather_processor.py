from scrape_weather import WeatherScraper
from dbcm import DBCM
from db_operations import DBOperations
from plot_operations import PlotOperations
import datetime

class WeatherProcessor:
  def __init__(self):
    self.scraper = WeatherScraper(
      base_url="http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
      )
    self.db_ops = DBOperations()
    self.plot_ops = PlotOperations()

  def run(self):
    """
    Main function that will run Weather processing program.
    """
    self.db_ops.initialize_db()
    while True:
      selection = self.show_menu()
      if selection == "1": # Selection 1: Download full data
        self.download_full_data()
      elif selection == "2": # Selection 2: Update latest data into database
        self.update_data()
      elif selection == "3": # Selection 3: Create box plot
        self.generate_boxplot()
      elif selection == "4": # Selection 4: Create line plot
        self.generate_lineplot()
      elif selection == "5": # Selection 5: Exit program
        print("Exiting program.")
        break
      else:
        print("Please pick appropriate option.")

  def show_menu(self):
    """
    Displays menu and receives input.
    """
    print("\nWeather Processor:")
    print("1. Download full Weather data.")
    print("2. Update Weather data.")
    print("3. Generate box plot")
    print("4. Generate line plot")
    print("5. Exit")
    return input("\nPick your selection: ").strip()

  def download_full_data(self):
    """
    Downloads full Weather data and will store it into database.
    """
    print("Downloading Weather data....")
    weather_data = self.scraper.scrape_weather_data()
    self.db_ops.save_data(weather_data, location="Winnipeg")
    print("Weather data downloaded and saved into database.")

  def update_data(self):
    """
    Updates Weather data.
    """
    print("Updating data....")
    latest_date = self.get_latest_date()
    today = datetime.date.today()
    if latest_date:
      start_date = (latest_date + datetime.timedelta(days=1)).isoformat()
      end_date = today.isoformat()
      print(f"Retrieving missing data from {start_date} to {end_date}")
      weather_data = self.scraper.scrape_weather_data()
      self.db_ops.save_data(weather_data, location="Winnipeg")
    else:
      print("No data found. Downloading full data....")
      self.download_full_data()

  def get_latest_date(self):
    """
    Gets latest date in the database.
    """
    query = """
        SELECT MAX(sample_date) FROM weather
    """
    with DBCM(self.db_ops.db_name) as conn:
      result = conn.execute(query).fetchone()
      return datetime.date.fromisoformat(result[0]) if result[0] else None

  def generate_boxplot(self):
    """
    Generate box plot within a year range by user.
    """
    print("Creating box plot....")
    try:
      start_year = int(input("Enter start year: ").strip())
      end_year = int(input("Enter end year: ").strip())
      start_date = f"{start_year}-01-01"
      end_date = f"{end_year}-01-01"
      db_data = self.db_ops.fetch_data(start_date=start_date, end_date=end_date)
      weather_data = prepare_boxplot_data(db_data)
      self.plot_ops.create_boxplot(weather_data, (start_year, end_year))
    except ValueError:
      print("Invalid years. Please enter valid years.")

  def generate_lineplot(self):
    """
    Generates line plot using month and year input by user.
    """
    print("Creating line plot....")
    try:
      year = int(input("Enter year: ").strip())
      month = int(input("Enter month (1-12): ").strip())
      start_date = f"{year}-{month:02d}-01"
      end_date = f"{year}-{month:02d}-31"
      db_data = self.db_ops.fetch_data(start_date=start_date, end_date=end_date)
      daily_temps = prepare_lineplot_data(db_data, month, year)
      if daily_temps:
        self.plot_ops.create_lineplot(daily_temps)
      else:
        print("No data available for this month and year.")
    except ValueError:
      print("Invalid input. Please enter a valid month and year.")

def prepare_lineplot_data(db_data, month, year):
  """
  Helper function - Receives data from database and prepares daily mean temperatures for line plotting.
  """
  daily_temps = []
  for sample_date, _, _, avg_temp in db_data:
    date_year, date_month, date_day = map(int, sample_date.split("-"))
    if date_year == year and date_month == month:
      daily_temps.append(avg_temp)
  return daily_temps


def prepare_boxplot_data(db_data):
  """
  Helper function - Receives data from database and groups mean temperatures by month for box plotting.
  """
  weather_data = {i: [] for i in range(1, 13)}
  for sample_date, _, _, avg_temp in db_data:
    month = int(sample_date.split("-")[1]) # Extract month from date
    weather_data[month].append(avg_temp)
  return weather_data

# Run Weather processor program
if __name__ == "__main__":
    processor = WeatherProcessor()
    processor.run()


