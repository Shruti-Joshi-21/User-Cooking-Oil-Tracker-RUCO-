# Spark image for this project = official Spark 3.5.3 + numpy (needed by Spark MLlib in Python).
FROM apache/spark:3.5.3-scala2.12-java17-python3-ubuntu

USER root
# numpy 1.x: PySpark 3.5 is tested against numpy 1.x
RUN pip install --no-cache-dir numpy==1.26.4
