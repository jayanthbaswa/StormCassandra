package dump;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import java.util.HashMap;
import java.util.Map;

public class CounterBolt implements IRichBolt {
    Map<String, Integer> counterMap;
    private OutputCollector collector;


    public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
        this.counterMap = new HashMap<String, Integer>();
        this.collector = collector;
    }


    public void execute(Tuple tuple) {
        String call = tuple.getString(0);

        Integer c =0;

        if(!counterMap.containsKey(call)){
            counterMap.put(call, 1);
        }else{
            c=counterMap.get(call) + 1;
            counterMap.put(call, c);
        }

        collector.emit(new Values(call, c.toString()));
        collector.ack(tuple);
    }


    public void cleanup() {
        for(Map.Entry<String, Integer> entry:counterMap.entrySet()){
            System.out.println(entry.getKey()+" : " + entry.getValue());
        }
    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("key", "value"));;
    }


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}