package fr.xebia.techevent.flume.gen;

import akka.actor.UntypedActor;
import akka.util.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Client coming from the main url (directly or from a search engine)
 */
public class StdClient extends AbstractClient {

    public StdClient(String ip, UserAgent userAgent, ApacheAccessLog apacheAccessLog) {
        super(apacheAccessLog, ip, userAgent);
    }

    @Override
    protected void processSession() {
        switch (lastState) {
            case BEGIN:
                // login
                // TODO Put a referer 50% empty 50% from searchengine
                // 100% login
                login();
                waitAround(5);
                break;
            case LOGIN:
                // logout and Wait 1 second
                // 100% Search
                search();
                waitAround(5);
                break;
            case SEARCH:
                // logout and Wait 1 second
                int t = r.nextInt(100);
                // 50% Search again
                if (t >= 0 && t < 50) {
                    search();
                    waitAround(5);
                }
                // 40% view
                if (t >= 50 && t < 90) {
                    view();
                    waitAround(5);
                }
                // 10% logout
                if (t >= 90 && t < 100) {
                    logout();
                }
                break;
            case VIEW:
                // logout and Wait 1 second
                int tv = r.nextInt(100);
                // 20% Buy
                if (tv >= 0 && tv < 20) {
                    buy();
                    waitAround(5);
                }
                // 60% Search another
                if (tv >= 20 && tv < 80) {
                    search();
                    waitAround(5);
                }
                // 20% logout
                if (tv >= 80 && tv < 100) {
                    logout();
                }
                break;
            case BUY:
                // logout and Wait 1 second
                int tb = r.nextInt(100);
                //95% Pay with success

                // 5% have a paiement error
                break;
            case PAY_SUCCESS:
            case PAY_ERROR:
                logout();
                break;
            case LOGOUT:
                // End of client
                // Todo need inform master
                break;
        }
    }


}
