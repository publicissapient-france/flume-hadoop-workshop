package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Count nb of error by type by minute
 */
public class AgregateErrorByMinute extends Reducer<CompoundKey<Text, IntWritable>, Text, CompoundKey<Text, IntWritable>, IntWritable> {

}
