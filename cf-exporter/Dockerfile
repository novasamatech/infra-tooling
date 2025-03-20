# Copyright © 2025 kogeler
# SPDX-License-Identifier: Apache-2.0

# Use the official Python image based on Alpine
FROM python:3.13-alpine

# Use environment variables so that Python won't create .pyc files
# and the output is written directly to stdout (unbuffered).
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Create a non-privileged user and group (pyuser)
# -D: do not create a home directory, -G: set the group
RUN addgroup -g 1000 pyuser && adduser -D -G pyuser -u 1000 pyuser

# Define the working directory inside the container
WORKDIR /app

# Copy the Python script into the container
COPY script.py .

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install dependencies without caching
RUN pip install --no-cache-dir -r requirements.txt

# Switch to the non-privileged user
USER pyuser

# By default, run the script
CMD ["python", "script.py"]
