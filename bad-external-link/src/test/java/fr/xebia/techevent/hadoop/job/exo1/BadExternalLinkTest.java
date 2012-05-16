package fr.xebia.techevent.hadoop.job.exo1;



import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

public class BadExternalLinkTest {

    MapDriver<LongWritable, Text, Text, Text> mapDriver;
    ReduceDriver<Text, Text, Text, Text> reduceDriver;

    @Before
    public void setUp() {
        BadExternalLinkMapper mapper = new BadExternalLinkMapper();
        BadExternalLinkReducer reducer = new BadExternalLinkReducer();

        mapDriver = new MapDriver<LongWritable, Text, Text, Text>();
        mapDriver.setMapper(mapper);

        reduceDriver = new ReduceDriver<Text, Text, Text, Text>();
        reduceDriver.setReducer(reducer);
    }

    @Test
    public void request_with_404_should_return_page_and_referer() {
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/May/2011:12:56:19 +0200] \"GET /non-existing.html HTTP/1.1\" 404 505 \"http://site-partenaire.com/content/article1.html\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.57 Safari/534.24"));
        mapDriver.withOutput(new Text("non-existing.html"), new Text("http://site-partenaire.com/content/article1.html"));
        mapDriver.runTest();
    }


    @Test
    public void request_other_than_404_should_return_nothing() {
        mapDriver.withInput(new LongWritable(1), new Text(
                "78.236.167.225 - - [09/May/2011:12:56:19 +0200] \"GET /existing.html HTTP/1.1\" 200 505 \"http://site-partenaire.com/content/article1.html\" \"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_6_6) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.57 Safari/534.24"));
        mapDriver.runTest();
    }

}
