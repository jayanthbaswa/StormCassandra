package dump;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;


public class ReaderSpout implements IRichSpout {
    //Create instance for SpoutOutputCollector which passes tuples to bolt.
    private SpoutOutputCollector collector;
    private boolean completed = false;

    //Create instance for TopologyContext which contains topology data.

    private TopologyContext context;

    //Create instance for Random class.
    private Random randomGenerator = new Random();
    private Integer idx = 0;


    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.context = context;
        this.collector = collector;
    }


    public void nextTuple() {
        String[] sentences = new String[]{ "thirteen",
                "Thursday",
                "princess",
                "assonant",
                "thousand",
                "fourteen",
                "language",
                "chipotle",
                "American",
                "business",
                "favorite",
                "elephant",
                "children",
                "birthday",
                "mountain",
                "feminine",
                "football",
                "kindness",
                "syllable",
                "abdicate",
                "treasure",
                "Virginia",
                "envelope",
                "strength",
                "together",
                "memories",
                "darkness",
                "February",
                "sandwich",
                "calendar",
                "bullying",
                "equation",
                "violence",
                "marriage",
                "building",
                "internal",
                "function",
                "November",
                "drooping",
                "abortion",
                "Victoria",
                "squirrel",
                "tomorrow",
                "champion",
                "sentence",
                "personal",
                "remember",
                "daughter",
                "hospital" };
        String sentence = sentences[randomGenerator.nextInt(sentences.length)];
        System.out.println(sentence);
        this.collector.emit(new Values(sentence));


    }


    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("words"));
    }

    //Override all the interface methods
    public void close() {}

    public boolean isDistributed() {
        return false;
    }


    public void activate() {}

    public void deactivate() {}

    public void ack(Object msgId) {}

    public void fail(Object msgId) {}


    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}