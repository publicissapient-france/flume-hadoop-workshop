package fr.xebia.techevent.hadoop.job;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

public abstract class CompoundKey<K1 extends WritableComparable, K2 extends WritableComparable> implements WritableComparable<CompoundKey<K1, K2>> {

    protected K1 k1;
    protected K2 k2;

    protected abstract Class k1Type();

    protected abstract Class k2Type();

    public CompoundKey() {
        try {
            this.k1 = (K1) k1Type().newInstance();
            this.k2 = (K2) k2Type().newInstance();
        } catch (InstantiationException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        } catch (IllegalAccessException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        CompoundKey that = (CompoundKey) o;

        if (!k1.equals(that.k1)) return false;
        if (!k2.equals(that.k2)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = k1.hashCode();
        result = 31 * result + k2.hashCode();
        return result;
    }
}
