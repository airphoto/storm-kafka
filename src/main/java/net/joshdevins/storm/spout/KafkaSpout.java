package net.joshdevins.storm.spout;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import org.apache.log4j.Logger;

import backtype.storm.spout.ISpout;
import backtype.storm.spout.Scheme;

/**
 * This spout can be used to consume messages from a cluster of Kafka brokers. The consumption and further processing in
 * the Storm topology is currently unreliable! When you create this spout in Storm, it is recommended that the
 * parallelism hint you set be less than or equal to the number of partitions available for the topic you are consuming
 * from. If you specify a parallelism hint that is great than the number of partitions, some spouts/consumers will sit
 * idle and not do anything. This design is inherent to the way Kafka works.
 * 
 * @author Josh Devins
 */
public class KafkaSpout extends BasicSchemeSpout {

    private static final long serialVersionUID = 1064554017925026658L;

    private final Properties kafkaProperties;

    private final String topic;

    private ConsumerConnector consumerConnector;

    private ConsumerIterator<byte[],byte[]> consumerIterator;

    /**
     * Default constructor. Actual Kafka consumers are be created and started on {@link #open()}.
     * 
     * @param kafkaProperties
     *        Properties to be used when constructing the Kafka {@link ConsumerConnector}.
     * @param topic
     *        The Kafka topic to consumer from.
     * @param scheme
     *        A {@link Scheme} that will be responsible for deserializing messages coming from Kafka. Whatever objects
     *        this produces needs to be natively understood by Storm or needs to be <a
     *        href="https://github.com/nathanmarz/storm/wiki/Serialization">registered with Storm</a> as a serializable
     *        type. Your best bet however is to stick to "native" types and use the internal tuple structure.
     */
    public KafkaSpout(final Properties kafkaProperties, final String topic, final Scheme scheme) {
        super(scheme);

        this.kafkaProperties = kafkaProperties;
        this.topic = topic;
    }

    /**
     * Cleanup the underlying Kafka consumer.
     */
    @Override
    public void close() {

        if (consumerConnector != null) {
            consumerConnector.shutdown();
        }
    }

    /**
     * Consume one message from the Kafka segment on the broker. As per the documentation on {@link ISpout}, this method
     * <strong>must</strong> be non-blocking if reliability measures are enabled/used, otherwise ack and fail messages
     * will be blocked as well. This current implementation is not reliable so we use a blocking consumer.
     */
    public void nextTuple() {

        // test to see if there is anything to be consumed, if not, sleep for a while
        // FIXME: this is blocking and as such, will always return true
        // if (!consumerIterator.hasNext()) {
        // Utils.sleep(50);
        // return;
        // }

        while(consumerIterator.hasNext())
        {
            emit(consumerIterator.next().message());
        }

    }

    /**
     * Create a Kafka consumer.
     */
    @Override
    public void open() {

        // these consumers use ZooKeeper for commit, offset and segment consumption tracking
        // TODO: consider using SimpleConsumer the same way the Hadoop consumer job does to avoid ZK dependency
        // TODO: use the task details from TopologyContext in the normal open method
        ConsumerConfig consumerConfig = new ConsumerConfig(kafkaProperties);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);

        // consumer with just one thread since the real parallelism is handled by Storm already
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic, new Integer(1));

        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector
                .createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);
        KafkaStream<byte[],byte[]> stream = streams.get(0);
        consumerIterator = stream.iterator();
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }


    public void activate() {

    }

    public void deactivate() {

    }

}
