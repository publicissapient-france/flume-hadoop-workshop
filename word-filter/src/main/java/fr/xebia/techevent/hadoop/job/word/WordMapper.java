package fr.xebia.techevent.hadoop.job.word;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import fr.xebia.techevent.hadoop.job.AccessLog;
import fr.xebia.techevent.hadoop.job.LogParser;

import java.io.IOException;

public class WordMapper extends Mapper<LongWritable, Text, Text, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AccessLog accessLog = LogParser.parseAccessLog(value.toString());
        if (accessLog!=null) {
        	Text logLine = value.toString().contains(context.getConfiguration().get("WORD_TO_BE_FILTERED")) ? new Text(value.toString()) : new Text();
            context.write(new Text(accessLog.resources), logLine);
        } 
    }
}
