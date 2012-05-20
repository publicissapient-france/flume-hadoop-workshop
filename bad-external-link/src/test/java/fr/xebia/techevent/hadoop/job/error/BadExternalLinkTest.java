package fr.xebia.techevent.hadoop.job.error;


import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

public class BadExternalLinkTest {

    MapDriver<LongWritable, Text, CompoundKey<Text, IntWritable>, Text> mapDriver;
    ReduceDriver<CompoundKey<Text, IntWritable>, Text, CompoundKey<Text, IntWritable>, IntWritable> reduceDriver;

    @Before
    public void setUp() {
        ErrorByTimeUnitMapper mapper = new ErrorByTimeUnitMapper();
        AgregateErrorByMinute reducer = new AgregateErrorByMinute();

        mapDriver = new MapDriver<LongWritable, Text, CompoundKey<Text, IntWritable>, Text>();
        mapDriver.setMapper(mapper);

        reduceDriver = new ReduceDriver<CompoundKey<Text, IntWritable>, Text, CompoundKey<Text, IntWritable>, IntWritable>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void request_with_404_should_return_page_and_referer() {
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /non-existing.html HTTP/1.1\" 404 505 \"http://site-partenaire.com/content/article1.html\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.57 Safari/534.24\""));
        mapDriver.withOutput(new TextIntCompoundKey(new Text("09/mai/2011:12:56"), new IntWritable(404)), new Text("/non-existing.html"));
        mapDriver.runTest();
    }


    @Test
    public void request_other_than_error_should_return_nothing() {
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] \"GET /existing.html HTTP/1.1\" 200 505 \"http://site-partenaire.com/content/article1.html\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.57 Safari/534.24\""));
        mapDriver.runTest();
    }

    @Test
    public void mapped_request_should_be_count() {
        reduceDriver.withInput(new TextIntCompoundKey(new Text("09/mai/2011:12:56"), new IntWritable(404)), Arrays.asList(new Text[]{new Text("/non-existing.html"), new Text("/non-existing-2.html")}));
        reduceDriver.withOutput(new TextIntCompoundKey(new Text("09/mai/2011:12:56"), new IntWritable(404)), new IntWritable(2));
    }

}
