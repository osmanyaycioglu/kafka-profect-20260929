kafka-topics.bat --create --bootstrap-server 127.0.0.1:9092 --topic first-topic --partitions 6 --replication-factor 3

kafka-topics.bat --create --bootstrap-server 127.0.0.1:9092 --topic second-topic --partitions 6 --replication-factor 2

kafka-topics.bat --describe --bootstrap-server 127.0.0.1:9092 --topic second-topic

kafka-console-producer.bat --topic first-topic --bootstrap-server 127.0.0.1:9092

kafka-topics.bat --describe --bootstrap-server 127.0.0.1:9092 --topic first-topic

kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files 00000000000000000000.log
