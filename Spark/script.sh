#!/bin/bash
/opt/spark/bin/spark-submit \
          --class org.example.MainKt \
          --master spark://spark-master:7077 \
          --deploy-mode client \
          /opt/app/Spark-1.0-SNAPSHOT-all.jar