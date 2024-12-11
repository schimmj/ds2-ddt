import json

# Load the JSON file
with open('weather_january.json', 'r') as file:
    weather_data = json.load(file)

# Process the data to reformat it
formatted_data = []
for entry in weather_data:
    formatted_entry = {
        "time": entry["validStart"],  # Using the "validStart" as "time"
        "tavg": entry["metdata"]["tavg"],
        "tx": entry["metdata"]["tx"],
        "tn": entry["metdata"]["tn"],
        "rhavg": entry["metdata"]["rhavg"],
        "rhx": entry["metdata"]["rhx"],
        "rhn": entry["metdata"]["rhn"],
        "td": entry["metdata"]["td"],
        "ddavg": entry["metdata"]["ddavg"],
        "ffavg": entry["metdata"]["ffavg"],
        "rr": entry["metdata"]["rr"],
        "lwavg": entry["metdata"]["lwavg"]
    }
    formatted_data.append(formatted_entry)

# Save the reformatted data to a new JSON file
with open('formatted_weather.json', 'w') as file:
    json.dump(formatted_data, file, indent=4)

print("Data has been reformatted and saved to 'formatted_weather.json'")
