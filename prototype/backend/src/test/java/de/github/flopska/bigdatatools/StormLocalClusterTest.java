package de.github.flopska.bigdatatools;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestJob;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.Debug;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class StormLocalClusterTest {

	@Test
	public void useTestingFramework() {
		Testing.withLocalCluster(new TestJob() {

			@Override
			public void run(ILocalCluster cluster) throws Exception {
				LocalDRPC drpc = new LocalDRPC();
				TridentTopology tridentTopology = new TridentTopology();
				FeederBatchSpout feederBatchSpout = new FeederBatchSpout(ImmutableList.of("word"));

				TridentState wordCount = tridentTopology.newStream("word_count", feederBatchSpout)
						.groupBy(new Fields("word"))
						.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
				
				tridentTopology.newDRPCStream("word_count", drpc).stateQuery(wordCount, new Fields("args"),
						new MapGet(), new Fields("count"));

//				wordCount.newValuesStream().filter(new Debug());

				cluster.submitTopology("testing", new Config(), tridentTopology.build());

				feederBatchSpout.feed(ImmutableList.of(new Values("test1")));

				System.out.println("============== OUTPUT ================");
				System.out.println("MyValue " + drpc.execute("word_count", "test1"));

				drpc.shutdown();
			}
		});
	}

}
