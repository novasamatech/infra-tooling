# Copyright © 2025 Novasama Technologies GmbH
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

# Copy the pinned requirements first, so the dependency layer stays cached
# when only the script changes
COPY requirements.txt .

# Install dependencies without caching
RUN pip install --no-cache-dir -r requirements.txt

# Preserve script access for pyuser regardless of the source file's permissions.
COPY --chmod=755 script.py .

# Switch to the non-privileged user
USER pyuser

# Prometheus metrics and /readyz (CF_EXPORTER_METRICS_PORT, default 8000)
EXPOSE 8000

# By default, run the script
CMD ["python", "script.py"]
