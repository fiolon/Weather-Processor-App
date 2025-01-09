import matplotlib.pyplot as plt
from db_operations import DBOperations
from scrape_weather import WeatherScraper

class PlotOperations:
  def __init__(self):
    pass

  def create_boxplot(self, weather_data, year_range):
    """
    Creates a box plot for mean temperatures in a given year range.
    """
    start_year, end_year = year_range
    months = list(range(1, 13)) # Range from month 1 to 12
    data = [weather_data.get(month, []) for month in months]

    plt.figure(figsize=(10, 6))
    plt.boxplot(data, patch_artist=True, showmeans=True, meanline=True)
    plt.xticks(months, [
      "Jan", "Feb", "Mar", "Apr", "May", "Jun",
      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
    ])
    plt.title(f"Mean Temperatures from {start_year} to {end_year}")
    plt.xlabel("Month")
    plt.ylabel("Temperatures (Celcius)")
    plt.grid(axis='y')
    plt.tight_layout()
    plt.show()

  def create_lineplot(self, daily_temps):
    """
    Creates a line plot for daily mean temperature of a particular month and year.
    """
    days = list(range(1, len(daily_temps) + 1))
    plt.figure(figsize=(10, 6))
    plt.plot(days, daily_temps, marker='o', linestyle='-', color='b', label="Mean Temp")
    plt.title(f"Average Daily Mean Temperatures")
    plt.xlabel("Day")
    plt.ylabel("Temperature (Celcius)")
    plt.xticks(days)
    plt.legend()
    plt.grid()
    plt.tight_layout()
    plt.show()

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

# Run PlotOperations
if __name__ == "__main__":
  # Initialize database operation
  db_ops = DBOperations()
  db_ops.initialize_db()

  # Scrape data
  base_url = "http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
  scraper = WeatherScraper(base_url)
  weather_data = scraper.scrape_weather_data()
  db_ops.save_data(weather_data, location="Winnipeg")

  # Fetch data in a given year range
  year_range = (2020, 2024)
  db_data = db_ops.fetch_data(start_date="2020-01-01", end_date="2024-12-31")

  # Prepare data for box plot
  weather_data = prepare_boxplot_data(db_data)

  # Box Plot for years 2020-2024
  plot_ops = PlotOperations()

  # Prepare data for box plot
  plot_ops.create_boxplot(weather_data, year_range)

  # Line plot for month of January 2023
  db_data = db_ops.fetch_data(start_date="2023-01-01", end_date="2023-01-31")
  daily_temps = prepare_lineplot_data(db_data, 1, 2023)

  plot_ops.create_lineplot(daily_temps)

  db_ops.purge_data() # Delete data but not database

