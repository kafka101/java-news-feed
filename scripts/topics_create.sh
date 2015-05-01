#!/bin/bash
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic sport_news --config cleanup.policy=compact --replication-factor 1 --partitions 1
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic business_news --config cleanup.policy=compact --replication-factor 1 --partitions 1