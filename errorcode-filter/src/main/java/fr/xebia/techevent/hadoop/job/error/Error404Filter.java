package fr.xebia.techevent.hadoop.job.error;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Error404Filter {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage : %s [generic options] <input dir> <output dir>\n", Error404Filter.class.getSimpleName());
            return;
        }
        Job job = new Job(new Configuration(), "Count 404 job");
        job.setJarByClass(Error404Filter.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + System.currentTimeMillis()));
        job.setMapperClass(Search404Mapper.class);
        job.setReducerClass(CountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
