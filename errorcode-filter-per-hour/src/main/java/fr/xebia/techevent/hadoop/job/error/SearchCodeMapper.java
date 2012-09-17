package fr.xebia.techevent.hadoop.job.error;

import fr.xebia.techevent.hadoop.job.AccessLog;
import fr.xebia.techevent.hadoop.job.LogParser;
import fr.xebia.techevent.hadoop.job.error.model.ErrorInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SearchCodeMapper extends Mapper<LongWritable, Text, ErrorInfo, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        AccessLog al = LogParser.parseAccessLog(value.toString());

        if (al != null && al.returnCode.equals(context.getConfiguration().get("ERROR_CODE"))) {
            ErrorInfo errorInfo = new ErrorInfo();
            errorInfo.setError(new Text(al.returnCode));
            errorInfo.setHour(extractHour(al));
            errorInfo.setResources(new Text(al.resources));

            context.write(errorInfo, new IntWritable(1));
        }
    }

    private Text extractHour(AccessLog al) {
        // 14/Sep/2012:09:26:56 +0200
        Pattern p = Pattern.compile(".*?:(.*?):.*");
        Matcher m = p.matcher(al.timestamp);
        if (m.matches()) {
            return new Text(m.group(1));
        }

        return null;
    }
}
