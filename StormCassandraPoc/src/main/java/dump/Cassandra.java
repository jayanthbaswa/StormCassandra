package dump;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import java.util.HashMap;

import static org.apache.storm.cassandra.DynamicStatementBuilder.*;

public class Cassandra {
    private static final String WORD_SPOUT = "WORD_SPOUT";
    private static final String COUNT_BOLT = "COUNT_BOLT";
    private static final String CASSANDRA_BOLT = "WORD_COUNT_CASSANDRA_BOLT";

    public static void main(String[] args) throws Exception {

        Config config = new Config();
        String configKey = "cassandra-config";
        HashMap<String, Object> clientConfig = new HashMap<String, Object>();
        //clientConfig.put(StormCassandraConstants.CASSANDRA_HOST, "localhost:9160");
        //c1lientConfig.put(StormCassandraConstants.CASSANDRA_KEYSPACE, Arrays.asList(new String [] {"stormks"}));
        config.put("cassandra.keyspace","output");
        config.put("cassandra.nodes","localhost");
        config.put("cassandra.port",9042);

        //IRichBolt cassandraBolt =  new BaseCassandraBolt<>() {}
        //CassandraWriterBolt cassandraWriterBolt = new CassandraWriterBolt(new )

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("words-spout", new ReaderSpout());

//        builder.setBolt("call-log-creator-bolt", new CallLogCreatorBolt())
//                .shuffleGrouping("words-spout");

//        builder.setBolt("call-log-counter-bolt", new CallLogCounterBolt())
//                .fieldsGrouping("call-log-creator-bolt", new Fields("words"));
        builder.setBolt("counter-bolt", new CounterBolt())
                .fieldsGrouping("words-spout",new Fields("words"));


        builder.setBolt("WORD_COUNT_CASSANDRA_BOLT", new CassandraWriterBolt(
                async(
                        simpleQuery("INSERT INTO output.bout  (key,value ) VALUES (?,?);")
                        .with(
                                fields("key","value")
                        )
                )
        ),1).globalGrouping("counter-bolt");//globalGrouping("call-log-counter-bolt");
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", config, builder.createTopology());
            Thread.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
            System.exit(0);


    }
}
