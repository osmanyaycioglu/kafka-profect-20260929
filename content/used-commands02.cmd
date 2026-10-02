kafka-topics --create --topic first-test --replication-factor 2 --bootstrap-server 127.0.0.1:9092

kafka-topics --describe --topic first-test --bootstrap-server 127.0.0.1:9092

kafka-topics --create --topic second-test --replication-factor 2 --bootstrap-server 127.0.0.1:9092 --partitions 10

kafka-topics --describe --topic second-test --bootstrap-server 127.0.0.1:9092

kafka-topics --create --topic third-test --replication-factor 3 --bootstrap-server 127.0.0.1:9092 --partitions 10

kafka-topics --describe --topic third-test --bootstrap-server 127.0.0.1:9092

kafka-run-class.bat kafka.tools.DumpLogSegments --deep-iteration --print-data-log --files 00000000000000000000.log

kafka-producer-perf-test.bat --topic third-test --num-records 1000000 --throughput -1 --record-size 1024 --producer.config ./config/producer.properties


