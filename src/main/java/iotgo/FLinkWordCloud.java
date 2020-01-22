package iotgo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class FLinkWordCloud {

    public static void main(String[] args) {
        ExecutionEnvironment executionEnvironment = ExecutionEnvironment.getExecutionEnvironment();


        DataSource<String> dataSource = executionEnvironment.fromElements("hello","world","hello","java");

        try {
            dataSource.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
                @Override
                public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                    collector.collect(new Tuple2<String,Integer>(s,1));
                }
            })
                    .groupBy(0)
                    .sum(1)
                    .print();
        }catch (Exception e){
            e.printStackTrace();
        }

    }
}
