FROM tabulario/spark-iceberg

# Install Hadoop AWS dependencies
RUN mkdir -p /opt/spark/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar /opt/spark/jars/

# Set environment variables
ENV SPARK_CLASSPATH="/opt/spark/jars/*"

CMD ["spark-shell"]
