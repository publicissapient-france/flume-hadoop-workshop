package fr.xebia.techevent.flume.gen;

import org.junit.Test;

public class TestApacheAccessLog {

    @Test
    public void testLogin() {
        ApacheAccessLog tal = new ApacheAccessLog();
        System.out.println(tal.login("10.0.0.1", UserAgent.FIREFOX_MAC_1, "test"));
    }

}
