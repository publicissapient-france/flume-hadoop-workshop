package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.error.model.ErrorInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class CountReducer extends Reducer<ErrorInfo, IntWritable, ErrorInfo, IntWritable> {
    @Override
    protected void reduce(ErrorInfo key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        Iterator<IntWritable> it = values.iterator();
        while (it.hasNext()) {
            count += it.next().get();
        }

        context.write(key, new IntWritable(count));
    }
}
