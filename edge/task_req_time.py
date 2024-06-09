import requests

# Define the API endpoint
url = 'http://192.168.10.145:5000/api'

# Define the request payload
payload = {'message': 'task3', 'time': 300}

# Send the POST request
response = requests.post(url, json=payload)

# Print the response
print(response.text)
