# DE5 lite: JupyterLab on top of our Spark 3.5.6 image.
# Reuses the bundled PySpark (/opt/spark/python) so the Spark version matches
# exactly (3.5.6). Iceberg/Paimon jars resolve from the shared spark-ivy-cache
# volume that spark-client already populated, so SparkSession startup is fast.

ARG SPARK_IMAGE=de5-spark:3.5.6
FROM ${SPARK_IMAGE}

USER root
RUN python3 -m pip install --no-cache-dir --upgrade pip \
 && python3 -m pip install --no-cache-dir \
      "jupyterlab>=4.0,<4.3" \
      "notebook>=7.0,<7.3" \
      "pandas<2.1"

# Custom "PySpark (DE5 Lakehouse)" kernel: catalogs pre-wired via PYSPARK_SUBMIT_ARGS,
# and a startup script auto-creates `spark`/`sc` so students need no boilerplate.
COPY docker/jupyter/kernel.json /home/spark/.local/share/jupyter/kernels/pyspark-de5/kernel.json
COPY docker/jupyter/00-spark-startup.py /home/spark/.ipython-pyspark/profile_default/startup/00-spark.py

# JupyterLab writes config/runtime under $HOME; make /home/spark writable by spark.
RUN mkdir -p /home/spark/.local /home/spark/.jupyter \
 && chown -R spark:spark /home/spark
ENV HOME=/home/spark

# Use the Spark-bundled PySpark (no separate pyspark install -> version match).
ENV PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

WORKDIR /workspace
USER spark
EXPOSE 8888
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.token=", "--ServerApp.password=", "--ServerApp.root_dir=/workspace"]
