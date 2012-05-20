package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Prend une ligne d'acces log
 * Retourne La minute et l'error code --> La page concerné
 */
public class ErrorByTimeUnitMapper extends Mapper<LongWritable, Text, CompoundKey<Text, IntWritable>, Text> {

}
