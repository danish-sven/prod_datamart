# Use Python 3.9 slim version as the base image
FROM python:3.9-slim

# Allow statements and log messages to immediately appear in the Knative logs
ENV PYTHONUNBUFFERED True

# Define the home directory for the app
ENV APP_HOME /app
WORKDIR $APP_HOME

# Copy the requirements file and install dependencies
COPY cloud-run/requirements.txt $APP_HOME/cloud-run/requirements.txt
RUN pip install --no-cache-dir -r $APP_HOME/cloud-run/requirements.txt

# Copy the entire application
COPY cloud-run/ $APP_HOME/cloud-run/

# Execute
CMD exec python $APP_HOME/cloud-run/app.py
