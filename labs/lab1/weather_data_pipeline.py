import requests
import csv

URL = "https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41&past_days=10&hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"


### Part 1. Read Operation (Extract)
def fetch_weather_data():
    """Fetches weather data for the past 10 days."""
    response = requests.get(URL)
    return response.json()

    ## TODO: complete the code, the output should be data in json format


def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    with open(filename, "w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        headers = ["time"] + list(data["hourly_units"].keys())[1:]
        writer.writerow(headers)
        hourly_data = data["hourly"]
        rows = zip(hourly_data["time"], *[hourly_data[key] for key in headers[1:]])
        writer.writerows(rows)

        print(f"Data has been saved to {filename}")
        return None


### Part 3. Cleaning Operation (Transform)
def clean_data(input_file, output_file):
    """clean the data based on the following rules:
    1. Temperature should be between 0 and 60°C
    2. Humidity should be between 0% and 80%
    3. Wind speed in a betweeen 3 and 150
    """

    ### TODO: complete rest of the code
    try:
        with open(input_file, "r") as infile, open(
            output_file, "w", newline=""
        ) as outfile:
            reader = csv.DictReader(infile)
            fieldnames = reader.fieldnames
            writer = csv.DictWriter(outfile, fieldnames=fieldnames)

            # Write the header to the output file
            writer.writeheader()

            # Process and filter rows
            for row in reader:
                try:
                    temperature = float(row["temperature_2m"])
                    humidity = float(row["relative_humidity_2m"])
                    wind_speed = float(row["wind_speed_10m"])

                    # Apply filtering conditions
                    if (
                        0 <= temperature <= 60
                        and 0 <= humidity <= 80
                        and 3 <= wind_speed <= 150
                    ):
                        writer.writerow(row)
                except ValueError:
                    # Skip rows with invalid numeric data
                    continue

        print("Cleaned data saved to", output_file)
    except Exception as e:
        print("An error occurred:", e)
    print("Cleaned data saved to", output_file)


### Part 4. Aggregation Operation
def summarize_data(filename):
    """Summarizes weather data including averages and extremes."""
    with open(filename, "r", encoding="utf-8") as file:
        reader = csv.reader(file)
        headers = next(reader)  # Read header row
        data = list(reader)  # Convert CSV data to list

        # Ensure we have data
        if not data:
            print("No data available to summarize.")
            return

        # Extract values from columns
        temperatures = [float(row[1]) for row in data if row[1]]
        humidity_values = [float(row[2]) for row in data if row[2]]
        wind_speeds = [float(row[3]) for row in data if row[3]]

        # Compute statistics
        ### TODO: complete rest of the code by computing the below mentioned metrics
        total_records = len(data)
        avg_temp = sum(temperatures) / len(temperatures) if temperatures else 0
        max_temp = max(temperatures) if temperatures else 0
        min_temp = min(temperatures) if temperatures else 0
        avg_humidity = (
            sum(humidity_values) / len(humidity_values) if humidity_values else 0
        )
        avg_wind_speed = sum(wind_speeds) / len(wind_speeds) if wind_speeds else 0

        # Print summary
        print("📊 Weather Data Summary 📊")
        print(f"Total Records: {total_records}")
        print(f"🌡️ Average Temperature: {avg_temp:.2f}°C")
        print(f"🔥 Max Temperature: {max_temp:.2f}°C")
        print(f"❄️ Min Temperature: {min_temp:.2f}°C")
        print(f"💧 Average Humidity: {avg_humidity:.1f}%")
        print(f"💨 Average Wind Speed: {avg_wind_speed:.2f} m/s")


if __name__ == "__main__":
    weather_data = fetch_weather_data()
    print("Weather data fetched successfully.", weather_data)
    if weather_data:
        save_to_csv(weather_data, "weather_data.csv")
        print("Weather data saved to weather_data.csv")
        clean_data("weather_data.csv", "cleaned_data.csv")
        print("Weather data clean saved to cleaned_data.csv")
        summarize_data("cleaned_data.csv")
