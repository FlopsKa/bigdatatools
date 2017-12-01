package de.github.flopska.bigdatatools;

import java.util.concurrent.TimeUnit;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.OpaqueTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.testing.CountAsAggregator;
import org.apache.storm.trident.windowing.InMemoryWindowsStoreFactory;
import org.apache.storm.trident.windowing.WindowsStoreFactory;
import org.apache.storm.trident.windowing.config.SlidingDurationWindow;
import org.apache.storm.tuple.Fields;

public class CountTopology {

	public static void main(String[] args) {

		TridentTopology topology = new TridentTopology();
		BrokerHosts zk = new ZkHosts("localhost");
		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "words");
		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
		WindowsStoreFactory mapState = new InMemoryWindowsStoreFactory();
		SlidingDurationWindow windowConfig = SlidingDurationWindow.of(
				new BaseWindowedBolt.Duration(10, TimeUnit.SECONDS),
				new BaseWindowedBolt.Duration(1, TimeUnit.SECONDS));
		topology.newStream("words", spout)
				.window(windowConfig, mapState, new Fields("str"), new CountAsAggregator(), new Fields("count"))
				.filter(new Debug());

		LocalCluster cluster = new LocalCluster("localhost", 2181L);
		cluster.submitTopology("localCluster2", new Config(), topology.build());
	}
}
