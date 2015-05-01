#!/bin/bash
kafka-topics.sh --zookeeper 127.0.0.1:2181 --create --topic news_topic --config cleanup.policy=compact --replication-factor 1 --partitions 1