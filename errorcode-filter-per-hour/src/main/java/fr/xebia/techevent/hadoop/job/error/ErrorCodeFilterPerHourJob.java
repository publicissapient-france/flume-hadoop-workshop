package fr.xebia.techevent.hadoop.job.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class ErrorCodeFilterPerHourJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage : %s [generic options] <error code to search> <input dir> <output dir>\n", ErrorCodeFilterPerHourJob.class.getSimpleName());
            return;
        }
        Job job = new Job(new Configuration(), "Count specific error code job");
        job.setJarByClass(ErrorCodeFilterPerHourJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + System.currentTimeMillis()));
        job.setMapperClass(SearchCodeMapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        job.getConfiguration().set("ERROR_CODE", args[0]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
