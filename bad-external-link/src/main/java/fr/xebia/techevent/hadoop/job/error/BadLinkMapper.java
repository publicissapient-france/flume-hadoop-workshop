package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Prend une ligne d'acces log
 * Retourne Le couple url - referer si error code 404
 */
public class BadLinkMapper extends Mapper<LongWritable, Text, CompoundKey<Text, Text>, IntWritable> {

}
