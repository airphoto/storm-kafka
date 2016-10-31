package net.joshdevins.storm.topologies;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import net.joshdevins.storm.spout.KafkaSpout;
import net.joshdevins.storm.spout.StringScheme;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

public class KafkaTestTopology {


    public static void main(final String[] args) {

        TopologyBuilder builder = new TopologyBuilder();

        // ensure that you have the same or more partitions on the Kafka broker
        // if parallelism count is greater than partitions, some spouts/consumers will sit idle
        builder.setSpout("kafkaspout", createKafkaSpout(), 3);

        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(Config.TOPOLOGY_DEBUG, true);


        if(args.length > 0)
        {
            try {
                StormSubmitter.submitTopology("storm-kafka",conf,builder.createTopology());
            } catch (AlreadyAliveException e) {
                e.printStackTrace();
            } catch (InvalidTopologyException e) {
                e.printStackTrace();
            }
        }

        else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());

            // for testing, just leave up for 10 mins then kill it all
            Utils.sleep(10 * 60 * 1000); // 10 mins
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

    private static IRichSpout createKafkaSpout() {

        // setup Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.put("zookeeper.connect", "192.168.0.103:2181");
        kafkaProps.put("zookeeper.connectiontimeout.ms", "1000000");
        kafkaProps.put("group.id", "storm");

        return new KafkaSpout(kafkaProps, "test", new StringScheme());
    }
}
