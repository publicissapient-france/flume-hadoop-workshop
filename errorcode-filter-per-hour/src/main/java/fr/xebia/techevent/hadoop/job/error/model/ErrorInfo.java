package fr.xebia.techevent.hadoop.job.error.model;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ErrorInfo implements WritableComparable<ErrorInfo> {

    private Text error;

    private Text resources;

    private Text hour;

    public ErrorInfo() {
        this.error = new Text();
        this.resources = new Text();
        this.hour = new Text();
    }

    public Text getError() {
        return error;
    }

    public void setError(Text error) {
        this.error = error;
    }

    public Text getResources() {
        return resources;
    }

    public void setResources(Text resources) {
        this.resources = resources;
    }

    public Text getHour() {
        return hour;
    }

    public void setHour(Text hour) {
        this.hour = hour;
    }

    public int compareTo(ErrorInfo o) {
        if (o != null) {
            int cmp = this.error.compareTo(o.error);

            if (cmp != 0) {
                return cmp;
            } else {

                cmp = this.resources.compareTo(o.resources);
                if (cmp != 0) {
                    return cmp;
                } else {
                    return this.hour.compareTo(o.hour);
                }
            }
        } else {
            return 0;
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        error.write(dataOutput);
        resources.write(dataOutput);
        hour.write(dataOutput);
    }

    public void readFields(DataInput dataInput) throws IOException {
        error.readFields(dataInput);
        resources.readFields(dataInput);
        hour.readFields(dataInput);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ErrorInfo that = (ErrorInfo) o;

        if (error != null ? !error.equals(that.error) : that.error != null) return false;
        if (hour != null ? !hour.equals(that.hour) : that.hour != null) return false;
        if (resources != null ? !resources.equals(that.resources) : that.resources != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = error != null ? error.hashCode() : 0;
        result = 31 * result + (resources != null ? resources.hashCode() : 0);
        result = 31 * result + (hour != null ? hour.hashCode() : 0);
        return result;
    }
}
