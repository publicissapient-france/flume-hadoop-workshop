package fr.xebia.techevent.hadoop.job;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CompoundKey<K1 extends WritableComparable, K2 extends WritableComparable> implements WritableComparable<CompoundKey<K1, K2>> {

    private K1 k1;
    private K2 k2;

    public CompoundKey(K1 k1, K2 k2) {
        this.k1 = k1;
        this.k2 = k2;
    }

    @Override
    public int compareTo(CompoundKey<K1, K2> ck) {
        return k1.compareTo(ck.getK1()) + k2.compareTo(ck.getK2());
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        k1.write(dataOutput);
        k2.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        k1.readFields(dataInput);
        k2.readFields(dataInput);
    }

    public K1 getK1() {
        return k1;
    }

    public K2 getK2() {
        return k2;
    }
}
