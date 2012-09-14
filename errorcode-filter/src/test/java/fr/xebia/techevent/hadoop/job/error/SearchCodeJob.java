package fr.xebia.techevent.hadoop.job.error;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class SearchCodeJob {

    MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        SearchCodeMapper mapper = new SearchCodeMapper();
        CountReducer reducer = new CountReducer();

        mapDriver = new MapDriver<LongWritable, Text, Text, IntWritable>();
        mapDriver.setMapper(mapper);

        reduceDriver = new ReduceDriver<Text, IntWritable, Text, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void request_with_404_should_count_1() {
        Configuration conf = new Configuration();
        conf.set("ERROR_CODE", "404");
        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /non-existing.html HTTP/1.1\" 404 505"));
        mapDriver.withOutput(new Text("/non-existing.html"), new IntWritable(1));
        mapDriver.runTest();
    }


    @Test
    public void request_other_than_error_should_return_nothing() {
        Configuration conf = new Configuration();
        conf.set("ERROR_CODE", "404");
        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /existing.html HTTP/1.1\" 200 505"));
        mapDriver.runTest();
    }

    @Test
    public void mapped_request_should_be_count() {
        reduceDriver.withInput(new Text("/non-existing.html"), Arrays.asList(new IntWritable[]{new IntWritable(1), new IntWritable(1)}));
        reduceDriver.withOutput(new Text("/non-existing.html"), new IntWritable(2));
        reduceDriver.runTest();
    }

}
