package fr.xebia.techevent.hadoop.job.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordFilter {

    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.printf("Usage : %s [generic options] <input dir> <output dir>\n", WordFilter.class.getSimpleName());
            return;
        }
        Job job = new Job(new Configuration(), "Filter logs by word job");
        job.setJarByClass(WordFilter.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1] + "/" + System.currentTimeMillis()));
        job.setMapperClass(WordMapper.class);
        
        job.setReducerClass(IdentityReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set("WORD_TO_BE_FILTERED", args[0]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
