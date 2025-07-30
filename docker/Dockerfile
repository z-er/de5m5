FROM python:3.11.3

# Determine the container's working directory
WORKDIR /app

# Copy the current directory contents into the container's working directory
COPY . /app

# Install dependencies
RUN pip install -r requirements.txt
RUN apt-get update

# Upon start, run the command `python ./data_reading.py`
ENTRYPOINT [ "python", "data_reading.py"]
