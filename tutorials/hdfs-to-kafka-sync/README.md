This sample application shows how to read lines from files in HDFS and write
them out to a Kafka topic. Each line of the input file is considered a separate
message. This app is designed for kafka 0.8.

The topic name, the name of the directory that is monitored for input
files, and other parameters are configurable in `META_INF/properties.xml`.
