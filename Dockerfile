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
# matching pyspark install via conda. pytest is installed separately (with
# deps) since --no-deps would also block pytest's own dependencies
# (iniconfig, pluggy, etc.).
COPY requirements-extra.txt /tmp/requirements-extra.txt
RUN pip install --no-cache-dir --no-deps -r /tmp/requirements-extra.txt \
    && pip install --no-cache-dir pytest>=8.0 \
    && pip install --no-cache-dir importlib-metadata

# Editable install of the dataforge_ai package so notebooks/tests can
# `import dataforge_ai` instead of duplicating Spark logic across notebooks.
# src/ and pyproject.toml are also bind-mounted at runtime (see
# docker-compose.yml) over these same paths -- local edits to
# src/dataforge_ai/*.py take effect without rebuilding the image, only a
# kernel restart is needed to re-import.
COPY pyproject.toml /home/jovyan/work/pyproject.toml
COPY src/ /home/jovyan/work/src/
RUN pip install --no-cache-dir --no-deps -e /home/jovyan/work

USER ${NB_UID}

WORKDIR /home/jovyan/work
