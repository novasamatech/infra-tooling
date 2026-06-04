# Copyright © 2026 Novasama Technologies GmbH
# SPDX-License-Identifier: Apache-2.0

# Single image for both entry points. It defaults to the Prometheus exporter;
# the one-shot CLI is reached by overriding the CMD (see the bottom of this file
# and README.md → Docker).

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

# Copy the requirements.txt file into the container
COPY requirements.txt .

# Install dependencies without caching
RUN pip install --no-cache-dir -r requirements.txt

# Copy the shared library, both entry points, the config template, and the
# version file (the exporter reads .version for coturn_exporter_build_info).
COPY turntest.py app.py exporter.py coturn-exporter.example.toml .version .

# Switch to the non-privileged user
USER pyuser

# Prometheus scrape endpoint (used by the default exporter CMD).
EXPOSE 9686

# ENTRYPOINT is the interpreter, so the CMD (script + args) is what you replace
# at `docker run` time. Default = the exporter; mount your config at
# /config/coturn-exporter.toml:
#
#   docker run --rm -p 9686:9686 \
#     -v "$PWD/coturn-exporter.toml:/config/coturn-exporter.toml:ro" <image>
#
# To run the one-shot CLI instead, override the CMD with `app.py` and its flags:
#
#   docker run --rm <image> app.py -H turn.example.com -u user -p pass --transport tcp
#
ENTRYPOINT ["python"]
CMD ["exporter.py", "-c", "/config/coturn-exporter.toml"]
