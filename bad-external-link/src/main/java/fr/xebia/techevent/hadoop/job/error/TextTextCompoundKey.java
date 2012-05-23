package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.CompoundKey;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

public class TextTextCompoundKey extends CompoundKey<Text, Text> {

    public TextTextCompoundKey() {
        super();
    }

    public TextTextCompoundKey(Text text, Text text2) {
        super(text, text2);
    }

    @Override
    protected Class k1Type() {
        return Text.class;
    }

    @Override
    protected Class k2Type() {
        return Text.class;
    }


}
