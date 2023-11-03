FROM python:3.11
LABEL authors="deep_sea"


# Set the working directory to /app
WORKDIR /py-db

# Copy the contents of the local directory to /app
COPY . /py-db

# Install any needed packages specified in requirements.txt
RUN pip install -r requirements.txt
