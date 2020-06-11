# Apache-Kafka-Streaming-Elasticsearch

Apache kafka project to read messages streaming from Twitter in real-time and store them in Elastisearch.

Utilizing Kafka as a distributed streaming platform to read streaming data from twitter.
It follows a publish-subscribe model where twitter tweets/messages are published to kafka Topic (by producer) and 
read/subscribed from the topic in kafka cluster (by consumer).
Messages are grouped into topics. As messages are consumed, they are removed from Kafka.
Eventually the consumed data is sent to the Elasticsearch. 
Data in the topic is also filtered by Kafka's Streaming API to send out tweets with users only having > 1000 followers.

The code includes:

1. Writing Kafka producers and consumers in Java
2. Writing and configuring a Twitter producer
3. Writing a Kafka consumer for ElasticSearch
4. Working with Kafka APIs: Kafka Connect, Streams
