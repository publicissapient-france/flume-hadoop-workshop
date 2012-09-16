package fr.xebia.techevent.hadoop.job;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class LogParserTest{
	
	@Test
	public void test_parseAccessLog() {
		String testedLog = "127.0.0.1 - - [14/Sep/2012:09:26:56 +0200] \"GET /docs/manager-howto.html HTTP/1.1\" 404 1034";
		AccessLog al = LogParser.parseAccessLog(testedLog);
		assertNotNull(al);
		assertEquals("127.0.0.1", al.ip);
		assertEquals("14/Sep/2012:09:26:56 +0200", al.timestamp);
		assertEquals("GET", al.command);
		assertEquals("/docs/manager-howto.html", al.resources);
		assertEquals("HTTP/1.1", al.protocol);
		assertEquals("404", al.returnCode);
		assertEquals("1034", al.processTime);
	}

}
