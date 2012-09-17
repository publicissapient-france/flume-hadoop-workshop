package fr.xebia.techevent.hadoop.job.word;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class WordFilterJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage : %s [generic options] <word to be filtered> <input dir> <output dir>\n", WordFilterJob.class.getSimpleName());
            return;
        }
        Job job = new Job(new Configuration(), "Filter logs by word job");
        job.setJarByClass(WordFilterJob.class);

        FileInputFormat.setInputPaths(job, new Path(args[1]));
        FileOutputFormat.setOutputPath(job, new Path(args[2] + "/" + System.currentTimeMillis()));
        job.setMapperClass(WordMapper.class);
        
        job.setReducerClass(Reducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.getConfiguration().set("WORD_TO_BE_FILTERED", args[0]);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
