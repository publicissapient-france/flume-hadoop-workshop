package fr.xebia.techevent.hadoop.job.word;

import static com.google.common.collect.Lists.newArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class WordFilterTest {
    private MapDriver<LongWritable, Text, Text, Text> mapDriver;
    private ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        Configuration conf = new Configuration();
        conf.set("WORD_TO_BE_FILTERED", "favicon");
        mapDriver = MapDriver.newMapDriver(new WordMapper()).withConfiguration(conf);
        reduceDriver = ReduceDriver.newReduceDriver(new IdentityReducer());
    }

    @Test
    public void request_with_favicon_should_return_trace() {
        mapDriver
                .withInput(
                        new LongWritable(1),
                        new Text("78.236.167.225 - - [13/Sep/2012:22:20:31 +0200] \"GET /favicon.ico HTTP/1.1\" 404 988"))
                .withOutput(
                        new Text("/favicon.ico"),
                        new Text("78.236.167.225 - - [13/Sep/2012:22:20:31 +0200] \"GET /favicon.ico HTTP/1.1\" 404 988"))
                .runTest();
    }

    @Test
    public void request_with_favicon_should_not_return_trace() {
        mapDriver
                .withInput(
                        new LongWritable(1),
                        new Text("78.236.167.225 - - [13/Sep/2012:22:20:33 +0200] \"GET /hotels/1 HTTP/1.1\" 200 2366"))
                .withOutput(new Text("/hotels/1"), new Text()).runTest();
    }

    @Test
    public void reduced_request_should_be_the_same_as_input() {
        reduceDriver
                .withInput(
                        new Text("/favicon.ico"),
                        newArrayList(new Text("78.236.167.225 - - [13/Sep/2012:22:20:31 +0200] \"GET /favicon.ico HTTP/1.1\" 404 988")))
                .withOutput(
                        new Text("/favicon.ico"),
                        new Text("78.236.167.225 - - [13/Sep/2012:22:20:31 +0200] \"GET /favicon.ico HTTP/1.1\" 404 988"))
                .runTest();
    }

}
