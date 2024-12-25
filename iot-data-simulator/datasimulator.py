import random

from faker import Faker

# Initialize Faker instance
fake = Faker()


# Function to generate IoT sensor data
def generate_iot_data() -> dict:
    """Generates simulated IoT sensor data with random values.

    Creates a dictionary representing a synthetic weather sensor measurement with randomized parameters. Provides realistic, varied data for testing and simulation purposes.

    Returns:
        dict: A dictionary containing simulated IoT sensor data with device ID, environmental measurements, location, status, and timestamp.
    """
    datetime_this_decade = fake.date_time_this_year(before_now=True)
    return {
        "device_id": f"sensor_{random.randint(1, 100)}",
        "temperature": round(random.uniform(-10.0, 50.0), 2),  # Temperature in Celsius
        "humidity": random.randint(10, 100),  # Humidity in percentage
        "pressure": round(random.uniform(950, 1050), 2),  # Atmospheric pressure in hPa
        "wind_speed": round(random.uniform(0.0, 30.0), 2),  # Wind speed in m/s
        "rainfall": round(random.uniform(0.0, 200.0), 2),  # Rainfall in mm
        "location": {
            "latitude": round(random.uniform(-90.0, 90.0), 6),  # Latitude
            "longitude": round(random.uniform(-180.0, 180.0), 6),  # Longitude
        },
        "device_status": random.choice(
            ["active", "inactive", "maintenance"]
        ),  # Status of the IoT device
        "timestamp": datetime_this_decade.isoformat(),  # ISO 8601 formatted timestamp
    }
