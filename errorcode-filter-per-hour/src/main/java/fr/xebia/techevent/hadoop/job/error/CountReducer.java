package fr.xebia.techevent.hadoop.job.error;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import fr.xebia.techevent.hadoop.job.error.model.ErrorInfo;


public class CountReducer extends Reducer<ErrorInfo, IntWritable, ErrorInfo, IntWritable> {
    @Override
    protected void reduce(ErrorInfo key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        // todo
    }
}
