.\kafka-topics.bat --create --bootstrap-server 127.0.0.1:9092 --topic first-topic --partitions 8 --replication-factor 3
.\kafka-topics.bat --create --bootstrap-server 127.0.0.1:9092 --topic second-topic --partitions 8 --replication-factor 2

kafka-console-producer.bat --topic first-topic --bootstrap-server 127.0.0.1:9092

kafka-topics.bat --describe --bootstrap-server 127.0.0.1:9092 --topic first-topic

kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files 00000000000000000000.log

.\kafka-producer-perf-test.bat --topic perf-topic-1-1  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-10-1  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-20-1  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-20-2  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-20-3  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-100-1a  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties

.\kafka-producer-perf-test.bat --topic perf-topic-20-2  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties --producer-props linger.ms=100

.\kafka-producer-perf-test.bat --topic perf-topic-20-2  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties --producer-props batch.size=32000 buffer.memory=67,108,864

.\kafka-producer-perf-test.bat --topic perf-topic-20-2  --num-records 50000  --throughput -1 --record-size 1024 --producer.config ..\..\config\producer.properties --producer-props batch.size=65,536 buffer.memory=67,108,864


.\kafka-consumer-perf-test.bat --topic perf-topic-20-2 --messages 50000 --consumer.config ..\..\config\consumer.properties --bootstrap-server localhost:9092
