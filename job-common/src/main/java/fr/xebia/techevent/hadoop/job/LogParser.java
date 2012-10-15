package fr.xebia.techevent.hadoop.job;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class LogParser {

    private static final String ACCESS_LOG_PATTERN = "" +
            ".*?" + //                                                      Match anything
            "([0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3}\\.[0-9]{1,3})" + //      Capture IP Address
            ".*?" + //                                                      Match anything
            "\\[(.+)\\]" + //                                               Capture the date between square brackets
            " \"(.*)" + //                                                  Capture the HTTP verb
            " (.*)" + //                                                    Capture the requested path
            " (.*)" + //                                                    Capture the protocol
            "\" ([0-9]{3})" + //                                            Capture the return code
            " ([0-9]+)" + //                                                Capture the process time
            ".*?"; //                                                       Match anything the ends the log

    /**
     * Parse log of type
     * <p/>
     * "127.0.0.1 - - [14/Sep/2012:09:26:56 +0200] "GET /docs/manager-howto.html HTTP/1.1" 404 1034"
     * "78.236.167.225 - - [09/mai/2011:12:56:19 +0200] "GET /non-existing.html HTTP/1.1" 404 505"
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
