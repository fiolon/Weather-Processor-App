from html.parser import HTMLParser
import requests
import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

class WeatherScraper(HTMLParser):
    """
    A class to scrape Winnipeg weather data (min, max, and mean temperatures)
    from Environment Canada's website using the HTMLParser library.
    """
    def __init__(self, base_url):
        super().__init__()
        self.base_url = base_url
        self.weather_data = {}
        self.in_table = False
        self.in_row = False
        self.current_date = None
        self.temp_data = []
        self.has_data = False

    def handle_starttag(self, tag, attrs):
        """
        Handle start tags in the HTML document.
        """
        attrs = dict(attrs)

        # Find the table
        if tag == "table" and "class" in attrs and "data-table" in attrs["class"]:
            self.in_table = True
            self.has_data = True

        # Find rows in the table
        if self.in_table and tag == "tr":
            self.in_row = True
            self.temp_data = []

        # Find date cells
        if self.in_row and tag == "th":
            self.current_date = None

        # Find temperature cells
        if self.in_row and tag == "td":
            self.temp_data.append("")

    def handle_endtag(self, tag):
        """
        Handle end tags in the HTML document.
        """
        # Process the end of a row
        if tag == "tr" and self.in_row:
            self.in_row = False
            if self.current_date and len(self.temp_data) >= 3:
                try:
                    max_temp = float(self.temp_data[0]) if self.temp_data[0] else None
                    min_temp = float(self.temp_data[1]) if self.temp_data[1] else None
                    mean_temp = float(self.temp_data[2]) if self.temp_data[2] else None
                    if max_temp is not None and min_temp is not None and mean_temp is not None:
                        self.weather_data[self.current_date] = {
                            "Max": max_temp,
                            "Min": min_temp,
                            "Mean": mean_temp,
                        }
                except ValueError:
                    pass

        # End the table
        if tag == "table":
            self.in_table = False

    def handle_data(self, data):
        """
        Handle data inside HTML tags.
        """
        data = data.strip()

        # Capture the date
        if self.in_row and self.current_date is None and data.isdigit():
            self.current_date = f"{self.year}-{self.month:02d}-{int(data):02d}"

        # Capture temperature data
        if self.in_row and self.temp_data:
            self.temp_data[-1] += data.strip()

    def fetch_data(self, year, month):
        """
        Fetches data from Environment Canada's website using year and month that are passed.
        """
        url = f"{self.base_url}&Year={year}&Month={month}"
        response = requests.get(url)
        if response.status_code != 200:
            print(f"Failed to fetch data for {year}-{month:02d}")
            return {}

        self.year = year
        self.month = month
        self.has_data = False
        self.feed(response.text)
        return self.weather_data.copy()

    def scrape_weather_data(self):
        """
        Scrape weather data starting from the current date and going backward until no data is available.
        """
        today = datetime.date.today()
        year = today.year
        month = today.month

        # Implemented threading to speed up scraping process
        tasks = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            while True:
                print(f"Scraping data for {year}-{month:02d}")
                tasks.append(executor.submit(self.fetch_data, year, month))

                # Move to previous month
                month -= 1
                if month == 0:
                    month = 12
                    year -= 1

                if year < 1995:
                    break

            for future in as_completed(tasks):
                try:
                    result = future.result()
                    self.weather_data(result)
                except Exception as e:
                    print(f"Error during scraping: {e}")

        return self.weather_data

# Run WeatherScraper class
if __name__ == "__main__":
    base_url = "http://climate.weather.gc.ca/climate_data/daily_data_e.html?StationID=27174&timeframe=2"
    scraper = WeatherScraper(base_url)
    weather_data = scraper.scrape_weather_data()
    print(weather_data)
