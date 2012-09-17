package fr.xebia.techevent.hadoop.job.error;


import fr.xebia.techevent.hadoop.job.error.model.ErrorInfo;
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

    MapDriver<LongWritable, Text, ErrorInfo, IntWritable> mapDriver;
    ReduceDriver<ErrorInfo, IntWritable, ErrorInfo, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        SearchCodeMapper mapper = new SearchCodeMapper();
        CountReducer reducer = new CountReducer();

        mapDriver = new MapDriver<LongWritable, Text, ErrorInfo, IntWritable>();
        mapDriver.setMapper(mapper);

        reduceDriver = new ReduceDriver<ErrorInfo, IntWritable, ErrorInfo, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void request_with_404_should_count_1() {
        Configuration conf = new Configuration();
        conf.set("ERROR_CODE", "404");
        mapDriver.setConfiguration(conf);
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /non-existing.html HTTP/1.1\" 404 505"));

        ErrorInfo expectedError = new ErrorInfo();
        expectedError.setHour(new Text("12"));
        expectedError.setResources(new Text("/non-existing.html"));
        expectedError.setError(new Text("404"));

        mapDriver.withOutput(expectedError, new IntWritable(1));
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
        ErrorInfo error = new ErrorInfo();
        error.setHour(new Text("12"));
        error.setResources(new Text("/non-existing.html"));
        error.setError(new Text("404"));

        reduceDriver.withInput(error, Arrays.asList(new IntWritable[]{new IntWritable(1), new IntWritable(1)}));
        reduceDriver.withOutput(error, new IntWritable(2));
        reduceDriver.runTest();
    }

}
