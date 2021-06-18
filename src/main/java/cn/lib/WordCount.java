package cn.lib;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

public class WordCount {

    public static void main(String[] args) {


        ExecutionEnvironment batch=ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> stringDataSource = batch.fromElements("hello a", "hello b");



    }
}
