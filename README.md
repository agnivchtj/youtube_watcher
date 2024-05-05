# YT Watcher Streaming App/Bot
In this project we are building a streaming data application that can push out live notifications (via Telegram) on updates to views, likes, favorites and comments on Youtube.

This is done in the following steps:
- Writing a Python script to fetch and process data using Youtube REST API
- Streaming data live into a Kafka topic
- Processing the incoming source data with ksqlDB, recording updates to statistics
- Streaming out live notifications via Telegram bot to user

## Setting up the Kafka instance

There are a few key steps that were followed in Kafka: https://confluent.cloud/
1) Set up a new cluster & schema registry
2) Create a ksqlDB cluster with global access and add queries for streams:
<!--
CREATE STREAM yt_videos (
    video_id VARCHAR KEY,
    video_timestamp VARCHAR,
    title VARCHAR,
    description VARCHAR,
    views INTEGER,
    likes INTEGER, 
    favorites INTEGER,
    comments INTEGER
) WITH (
    KAFKA_TOPIC = 'yt_videos', 
    PARTITIONS = 1, 
    VALUE_FORMAT = 'avro'
);
-->

3) Write up code to stream API data into Kafka (see below)
This first requires installation of a few Python libraries:
<!-- pip install confluent_kafka -->
<!-- pip install fastavro -->
This also requires the following details from Kafka UI (stored in <!--config.py-->):
- Bootstrap server endpoint
- API key credentials for cluster
- API endpoint for schema registry
- API key credentials for schema registry

4) Check if data is being sent to the ksqlDB instance:
<!--
    SELECT * FROM YT_VIDEOS
    EMIT CHANGES;
-->

5) Create a new table for changes in statistics of the videos:
<!--
    CREATE TABLE yt_changes WITH (
        KAFKA_TOPIC = 'yt_changes'
    ) AS 
    SELECT
        video_id, 
        latest_by_offset(title) AS title, 
        latest_by_offset(views, 2)[1] AS prev_views, 
        latest_by_offset(views, 2)[2] AS curr_views, 
        latest_by_offset(likes, 2)[1] AS prev_likes, 
        latest_by_offset(likes, 2)[2] AS curr_likes, 
        latest_by_offset(favorites, 2)[1] AS prev_favorites,
        latest_by_offset(favorites, 2)[2] AS curr_favorites, 
        latest_by_offset(comments, 2)[1] AS prev_comments, 
        latest_by_offset(comments, 2)[2] AS curr_comments
    FROM YT_VIDEOS
    GROUP BY video_id;
-->

This can be checked by running script and going over to one of the videos:
<!-- 
    SELECT * FROM YT_CHANGES
    WHERE prev_views <> curr_views
    EMIT CHANGES;
-->

6) Set up a new Telegram bot with <!--/newbot--> and run the following in script:
<!-- curl https://api.telegram.org/bot<token>/getUpdates -->

This provides the <!--chat_id--> which is required for the final step (step 9).

The <!--<token>--> is provided when you create the bot in Telegram.

7) Create a new stream for outbound data meant for Telegram:
<!-- 
    CREATE STREAM telegram_outbound (
        `chat_id` VARCHAR,
        `text` VARCHAR
    ) WITH (
        KAFKA_TOPIC = 'telegram_outbound', 
        PARTITIONS = 1, 
        VALUE_FORMAT = 'avro'
    );
-->

8) Make use of HTTP Sink connector in Kafka to send outbound data to Telegram bot.
HTTP URL: <!-- https://api.telegram.org/bot<token>/sendMessage -->

9) Create a new stream that borrows data from <!--YT_CHANGES--> to stream the table events:
<!-- 
    CREATE STREAM yt_changes_stream WITH (
        KAFKA_TOPIC = 'yt_changes', 
        VALUE_FORMAT = 'avro'
    );
-->

10) Insert messages from <!--YT_CHANGES_STREAM--> into the outbound stream <!--TELEGRAM_OUTBOUND--> created earlier.
Running our script will then cause bot to report changes in likes for example:
<!-- 
    INSERT INTO telegram_outbound
    SELECT '<chat_id>' AS `chat_id`, CONCAT('Video: ', title, ' | Likes changed: ', CAST(prev_likes AS STRING), ' -> ', CAST(curr_likes AS STRING)) AS `text`
    FROM yt_changes_stream
    WHERE curr_likes <> prev_likes;
-->

## Streaming data into Kafka

The following imports are needed to stream data to Kafka instance:

<!--
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.serialization import StringSerializer
from confluent_kafka.schema_registry.avro import AvroSerializer
-->

Connection to the Kafka cluster is set up and the producer is configured to send data to the topic. It does this by fetching the Avro schema for <!--yt_videos--> from Schema Registry. Then, the Kafka producer is configured to use this schema for serializing the data before sending it to Kafka. This ensues that the data adheres to a predefined structure and can be efficiently stored from Kafka topics.

<!--
    schema_registry_client = SchemaRegistryClient(config['schema_registry'])
    yt_videos_value_schema = schema_registry_client.get_latest_version('yt_videos-value')

    kafka_config = config['kafka'] | {
        'key.serializer': StringSerializer(), 
        'value.serializer': AvroSerializer(
            schema_registry_client, 
            yt_videos_value_schema.schema.schema_str
        )
    }

    producer = SerializingProducer(kafka_config)
-->

Each message sent by the producer to the Kafka topic contains information namely the id, timestamp, title, description, views, likes, favorites and comments. The 'on_delivery' parameter specifies a callback function to be executed when the message is successfully delivered to the Kafka broker.

<!-- 
    producer.produce(
        topic = 'yt_videos', 
        key = video_id, 
        value = {
            'VIDEO_TIMESTAMP': video_item['snippet']['publishedAt'], 
            'TITLE': video_item['snippet']['title'], 
            'DESCRIPTION': video_item['snippet']['description'], 
            'VIEWS': int(video_item['statistics']['viewCount']), 
            'LIKES': int(video_item['statistics']['likeCount']), 
            'FAVORITES': int(video_item['statistics']['favoriteCount']), 
            'COMMENTS': int(video_item['statistics']['commentCount'])
        }, 
        on_delivery = on_delivery
    )
-->