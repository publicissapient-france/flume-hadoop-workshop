package fr.xebia.techevent.flume.gen;

import akka.actor.UntypedActor;
import akka.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: jburet
 * Date: 15/05/12
 * Time: 17:14
 * To change this template use File | Settings | File Templates.
 */
public abstract class AbstractClient extends UntypedActor {
    protected static final Logger APACHE_ACCESS_LOGGER = LoggerFactory.getLogger("ApacheAccLog");
    protected final String ip;
    protected final UserAgent userAgent;
    protected final ApacheAccessLog apacheAccessLog;
    protected final Random r = new Random();
    protected String sessionId;
    protected ClientState lastState = ClientState.BEGIN;

    public AbstractClient(ApacheAccessLog apacheAccessLog, String ip, UserAgent userAgent) {
        this.apacheAccessLog = apacheAccessLog;
        this.ip = ip;
        this.userAgent = userAgent;
    }

    protected abstract void processSession();

    protected void waitAround(int timeToWait) {
        // Wait around time to wait
        Random random = new Random();
        double t = timeToWait + (random.nextGaussian() - 0.5) * ((double) timeToWait);
        getContext().system().scheduler().scheduleOnce(Duration.create(t, TimeUnit.SECONDS), self(), "CONTINUE");
    }

    protected void login() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.login(ip, userAgent, sessionId));
        this.sessionId = UUID.randomUUID().toString();
        lastState = ClientState.LOGIN;
    }

    protected void logout() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.logout(ip, userAgent, sessionId));
        this.sessionId = null;
        lastState = ClientState.LOGOUT;
    }

    protected void view() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.view(ip, userAgent, sessionId));
        lastState = ClientState.VIEW;
    }

    protected void search() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.search(ip, userAgent, sessionId));
        lastState = ClientState.SEARCH;
    }

    protected void buy() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.buy(ip, userAgent, sessionId));
        lastState = ClientState.BUY;
    }

    protected void paySucess() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.paySuccess(ip, userAgent, sessionId));
        lastState = ClientState.PAY_SUCCESS;
    }

    protected void payError() {
        APACHE_ACCESS_LOGGER.info(apacheAccessLog.payError(ip, userAgent, sessionId));
        lastState = ClientState.PAY_ERROR;
    }

    @Override
    public void onReceive(Object o) throws Exception {
        // Peut importe ce que l'on recoit on continue l'execution
        processSession();
    }
}
