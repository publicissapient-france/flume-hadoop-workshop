package fr.xebia.techevent.hadoop.job.error;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Text currentKey = key;

        int count = 0;
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            count += it.next().get();
        }

        context.write(currentKey, new IntWritable(count));
    }
}
