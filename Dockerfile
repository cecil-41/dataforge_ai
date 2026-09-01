# Base image: Jupyter's official PySpark stack.
# It already bundles a matching JDK, Spark, Hadoop client libs (incl. winutils
# equivalents aren't needed since we run Linux inside the container), and
# Jupyter Notebook/Lab -- this is what saves us from the Windows Java/Hadoop
# setup pain we hit running Spark natively on Windows.
FROM quay.io/jupyter/pyspark-notebook:spark-3.5.3

USER root

# Extra Python packages not already in the base image.
# --no-deps avoids delta-spark's pyspark>=3.5.3 dependency pulling down a
# fresh ~300MB pyspark wheel via pip -- the base image already provides a
# matching pyspark install via conda.
COPY requirements-extra.txt /tmp/requirements-extra.txt
RUN pip install --no-cache-dir --no-deps -r /tmp/requirements-extra.txt \
    && pip install --no-cache-dir importlib-metadata

USER ${NB_UID}

WORKDIR /home/jovyan/work
