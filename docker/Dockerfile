FROM python:3.11.3

# Determine the container's working directory
WORKDIR /app

# Copy the current directory contents into the container's working directory
COPY . /app

# Upon start, run the command `python ./docker_demo.py`
ENTRYPOINT [ "python", "./docker_demo.py" ]
