FROM tabulario/spark-iceberg

# Install Hadoop AWS dependencies
RUN mkdir -p /opt/spark/jars
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.1/hadoop-aws-3.3.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.11.901/aws-java-sdk-bundle-1.11.901.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-azure/3.3.1/hadoop-azure-3.3.1.jar /opt/spark/jars/
ADD https://repo1.maven.org/maven2/com/azure/azure-storage-blob/12.8.0/azure-storage-blob-12.8.0.jar /opt/spark/jars/ 

# copy default configuration file
WORKDIR .
COPY spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY requirements.txt /opt/spark/work-dir/

RUN pip install -r /opt/spark/work-dir/requirements.txt

# Set environment variables
ENV SPARK_CLASSPATH="/opt/spark/jars/*"



