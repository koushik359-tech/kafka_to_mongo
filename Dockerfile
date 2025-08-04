# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any dependencies your consumer script might need
RUN pip install -r requirements.txt


# Make port 80 available to the world outside this container
# (Update this if your consumer script uses a different port)
EXPOSE 80

# Run your script when the container launches
CMD ["python", "confluent_avro_data_consumer.py"]
