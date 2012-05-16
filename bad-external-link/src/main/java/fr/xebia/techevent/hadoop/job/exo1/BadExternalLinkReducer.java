package fr.xebia.techevent.hadoop.job.exo1;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

public class BadExternalLinkReducer extends Reducer<Text, Text, Text, Text> {

}
