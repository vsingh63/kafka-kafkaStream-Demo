package com.credit.docs.publishFileToTopic.service;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;

public class StreamAPIService {

	public void processStreamData() {
		StreamsConfig streamsConfig = new StreamsConfig(getStreamConfig());
		final Serde<String> stringSerde = Serdes.String();
		final Serde<byte[]> byteArraySerde = Serdes.ByteArray();
		final KStreamBuilder builder = new KStreamBuilder();
		final KStream<byte[], String> creditDocs = builder.stream(byteArraySerde, stringSerde, "credit-check-docs");
		final KStream<byte[], String> transformed = creditDocs
				.map((key, value) -> KeyValue.pair(key, value.toUpperCase()));
		transformed.to(byteArraySerde, stringSerde, "streamed-out-topic");
		KafkaStreams stream = new KafkaStreams(builder, streamsConfig);
		stream.start();
		Runtime.getRuntime().addShutdownHook(new Thread(stream::close));

	}

	private Properties getStreamConfig() {
		Properties props = new Properties();
		props.put(StreamsConfig.CLIENT_ID_CONFIG, "stream-api-test");
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-credit-check");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
		return props;

	}

}
