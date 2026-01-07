FROM apache/flink:1.19.1-scala_2.12

# -----------------------------
# Switch to root for setup
# -----------------------------
USER root

# -----------------------------
# Install Python & pip
# -----------------------------
RUN apt-get update && \
    apt-get install -y \
        python3 \
        python3-pip \
        python3-distutils && \
    ln -sf /usr/bin/python3 /usr/bin/python && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# -----------------------------
# Install Python dependencies
# -----------------------------
RUN pip3 install --no-cache-dir \
    ruamel.yaml

# -----------------------------
# Copy PyFlink application code
# -----------------------------
# This becomes the Python package root
COPY flink /opt/flink/app

# -----------------------------
# Copy Flink connector JARs
# IMPORTANT: /opt/flink/lib is auto-loaded by Flink
# -----------------------------
COPY flink/usrlib/*.jar /opt/flink/lib/
COPY flink/usrlib/hadoop/*.jar /opt/flink/lib/

# -----------------------------
# Fix ownership for Flink runtime
# -----------------------------
RUN chown -R flink:flink /opt/flink/app /opt/flink/lib

# -----------------------------
# Switch back to flink user
# -----------------------------
USER flink

# -----------------------------
# Ensure Python imports work
# -----------------------------
ENV PYTHONPATH=/opt/flink/app