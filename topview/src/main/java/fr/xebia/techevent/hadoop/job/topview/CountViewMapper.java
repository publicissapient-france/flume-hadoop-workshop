package fr.xebia.techevent.hadoop.job.topview;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Filter by url and count
 */
public class CountViewMapper extends Mapper<IntWritable, Text, Text, IntWritable> {

    private String filter = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        filter = context.getConfiguration().get("url-filter");
    }

    @Override
    protected void map(IntWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Parse log for url
        if (filter != null) {
            // if filter present verify log begin with it
        }
        // Send to context
    }
}
