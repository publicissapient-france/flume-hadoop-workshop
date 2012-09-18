package fr.xebia.techevent.hadoop.job;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {

    private static final String ACCESS_LOG_PATTERN = ".* .*   ([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}) - - \\[(.*)\\] \"(.*) (.*) (.*)\" ([0-9]{3}) ([0-9]*)\\\\";

    /**
     * Parse log of type "127.0.0.1 - - [14/Sep/2012:09:26:56 +0200] "GET /docs/manager-howto.html HTTP/1.1" 404 1034"
     */
    public static AccessLog parseAccessLog(String log) {
        Pattern p = Pattern.compile(ACCESS_LOG_PATTERN);
        Matcher m = p.matcher(log);
        if (m.matches()) {
            return new AccessLog(m.group(1), m.group(2), m.group(3), m.group(4), m.group(5), m.group(6), m.group(7));
        }
        return null;
    }

}
