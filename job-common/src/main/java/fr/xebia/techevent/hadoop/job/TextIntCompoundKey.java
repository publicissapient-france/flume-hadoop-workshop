package fr.xebia.techevent.hadoop.job;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TextIntCompoundKey extends CompoundKey<Text, IntWritable> {

    public TextIntCompoundKey() {
        super();
    }

    public TextIntCompoundKey(Text text, IntWritable intWritable) {
        super(text, intWritable);
    }

    @Override
    protected Class k1Type() {
        return Text.class;
    }

    @Override
    protected Class k2Type() {
        return IntWritable.class;
    }


}
