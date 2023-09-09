package com.garmes.flink.kafka;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

public class MyFlatMapFunction implements FlatMapFunction<String, String> {

    @Override
    public void flatMap(String value, Collector<String> collector) throws Exception {
        // normalize and split the line
        String[] tokens = value.toLowerCase().split("\\W+");
        // emit the pairs
        for (String token : tokens) {
            if (token.length() > 0) {
                collector.collect(token);
            }
        }
    }
}