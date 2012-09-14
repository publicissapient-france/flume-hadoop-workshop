package fr.xebia.techevent.hadoop.job;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestLogParser {

    @Test
    public void test_parseAccessLog() {
        String testedLog = "127.0.0.1 - - [14/Sep/2012:09:26:56 +0200] \"GET /docs/manager-howto.html HTTP/1.1\" 404 1034";
        AccessLog al = LogParser.parseAccessLog(testedLog);
        assertNotNull(al);
        assertEquals(al.ip, "127.0.0.1");
        assertEquals(al.timestamp, "14/Sep/2012:09:26:56 +0200");
        assertEquals(al.command, "GET");
        assertEquals(al.resources, "/docs/manager-howto.html");
        assertEquals(al.protocol, "HTTP/1.1");
        assertEquals(al.returnCode, "404");
        assertEquals(al.processTime, "1034");

    }
}
