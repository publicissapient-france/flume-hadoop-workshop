package fr.xebia.techevent.hadoop.job.topview;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Take Url -> Count and transform it to Count -> URL
 */
public class TopMapper extends Mapper<Text, IntWritable, IntWritable, Text> {

    @Override
    protected void map(Text key, IntWritable value, Context context) throws IOException, InterruptedException {
        context.write(value, key);
    }
}
