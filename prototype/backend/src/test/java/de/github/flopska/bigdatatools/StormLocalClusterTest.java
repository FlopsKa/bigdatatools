package de.github.flopska.bigdatatools;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

import org.apache.storm.Config;
import org.apache.storm.ILocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.Testing;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.testing.TestJob;
import org.apache.storm.trident.TridentState;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.operation.builtin.Count;
import org.apache.storm.trident.operation.builtin.MapGet;
import org.apache.storm.trident.testing.FeederBatchSpout;
import org.apache.storm.trident.testing.MemoryMapState;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;

import de.github.flopska.bigdatatools.dto.KafkaWord;

public class StormLocalClusterTest {

	Gson gson = new Gson();
	FeederBatchSpout feederBatchSpout;

	@Test
	@Ignore
	public void feedKafkaMessagesToTopology() throws Exception {
		KafkaWord word = new KafkaWord("foobar", "foobar", System.currentTimeMillis() - 200,
				System.currentTimeMillis());
		String json = gson.toJson(word);
	}

	@Test
	public void canCountTotalOfMessages() {
		LocalDRPC drpc = new LocalDRPC();

		StormTopology topology = initTopology(drpc);

		Testing.withLocalCluster(new TestJob() {

			@Override
			public void run(ILocalCluster cluster) throws Exception {
				cluster.submitTopology("testing", new Config(), topology);

				feederBatchSpout.feed(ImmutableList.of(new Values("test1")));
				assertThat(drpc.execute("word_count", "total_count"), is("[[\"total_count\",1]]"));
				
				feederBatchSpout.feed(ImmutableList.of(new Values("test2")));
				feederBatchSpout.feed(ImmutableList.of(new Values("test3")));
				assertThat(drpc.execute("word_count", "total_count"), is("[[\"total_count\",3]]"));
			
				drpc.shutdown();
			}
		});
	}

	private StormTopology initTopology(LocalDRPC drpc) {
		feederBatchSpout = new FeederBatchSpout(ImmutableList.of("word"));
		TridentTopology tridentTopology = new TridentTopology();
		TridentState wordCount = tridentTopology.newStream("word_count", feederBatchSpout).each(new Fields("word"), new RenameToTotalCount(), new Fields("total_count")).groupBy(new Fields("total_count"))
				.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"));
		tridentTopology.newDRPCStream("word_count", drpc).stateQuery(wordCount, new Fields("args"), new MapGet(),
				new Fields("count"));
		return tridentTopology.build();
	}

	public static class RenameToTotalCount extends BaseFunction {

		@Override
		public void execute(TridentTuple tuple, TridentCollector collector) {
			collector.emit(new Values("total_count"));

		}

	}

}
