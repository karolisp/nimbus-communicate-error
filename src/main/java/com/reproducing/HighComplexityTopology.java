package com.reproducing;

import org.apache.storm.*;
import org.apache.storm.task.*;
import org.apache.storm.topology.*;
import org.apache.storm.topology.base.*;
import org.apache.storm.tuple.*;
import org.apache.storm.utils.*;

import java.util.*;

public class HighComplexityTopology {

    public static class ExclamationBolt extends BaseRichBolt {
        OutputCollector _collector;

        public void prepare(Map conf, TopologyContext context, OutputCollector collector) {
            _collector = collector;
        }

        public void execute(Tuple tuple) {
            _collector.emit(tuple, new Values(tuple.getString(0) + "!!!"));
            _collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer declarer) {
            declarer.declare(new Fields("word"));
        }


    }

    public static void main(String[] args) throws Exception {
        TopologyBuilder builder = new TopologyBuilder();
        int numberOfBolts = 400;
        String boltNameBase = "muchLongerIdThanExclamaitionIs";
        builder.setSpout("word", new TestWordSpout(), 10);
        builder.setBolt(boltNameBase+"1", new ExclamationBolt(), 3).shuffleGrouping("word");
        for(int i=2;i<numberOfBolts;i++){ //should result in 400 bolts
            builder.setBolt(boltNameBase+i, new ExclamationBolt(), 2).shuffleGrouping(boltNameBase+(i-1));
        }
        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            try {
                cluster.submitTopology("test", conf, builder.createTopology());
                Utils.sleep(10000);
            }
            finally {
                cluster.shutdown();
            }
        }
    }
}