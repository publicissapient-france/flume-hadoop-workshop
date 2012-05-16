package fr.xebia.techevent.flume.gen;

import java.util.Random;

public enum UserAgent {

    FIREFOX_MAC_1("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.7; rv:11.0) Gecko/20100101 Firefox/11.0"),
    FIREFOX_MAC_2("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:11.0) Gecko/20100101 Firefox/11.0"),
    FIREFOX_MAC_3("Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1"),
    OLD("Mozilla/4.0 (compatible;)"),
    UBUNTU_CHROME("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/12.04 Chromium/18.0.1025.151 Chrome/18.0.1025.151 Safari/535.19"),
    UBUNTU_CHROME_2("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/535.19 (KHTML, like Gecko) Ubuntu/11.10 Chromium/18.0.1025.151 Chrome/18.0.1025.151 Safari/535.19"),
    FIREFOX_XP("Mozilla/5.0 (Windows NT 5.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1"),
    FIREFOX_XP_2("Mozilla/5.0 (Windows; U; Windows NT 5.1; fr; rv:1.9.1.9) Gecko/20100315 Firefox/3.5.9"),
    FIREFOX_XP_3("Mozilla/5.0 (Windows; U; Windows NT 6.1; fr; rv:1.9.2.17) Gecko/20110420 Firefox/3.6.17"),
    FIREFOX_CHROME("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/534.24 (KHTML, like Gecko) Chrome/11.0.696.65 Safari/534.24"),
    IPAD("Mozilla/5.0 (iPad; CPU OS 5_1 like Mac OS X) AppleWebKit/534.46 (KHTML, like Gecko) Version/5.1 Mobile/9B176 Safari/7534.48.3"),
    GOOGLE_BOT("Mozilla/5.0 (compatible; Googlebot/2.1; +http://www.google.com/bot.html)"),
    YANDEX_BOT("Mozilla/5.0 (compatible; YandexBot/3.0; +http://yandex.com/bots)");

    public final String userAgent;

    private final static Random r = new Random();

    private UserAgent(String userAgent) {
        this.userAgent = userAgent;
    }

    public static UserAgent random() {
        UserAgent[] value = values();
        // Minus 2 bot
        return value[r.nextInt(value.length-2)];
    }

}
