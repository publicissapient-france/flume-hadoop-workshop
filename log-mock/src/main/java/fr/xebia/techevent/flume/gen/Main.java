package fr.xebia.techevent.flume.gen;

import akka.actor.*;

public class Main {

    private ApacheAccessLog aal = new ApacheAccessLog();

    public static void main(String[] args) {
        Main m = new Main();
        m.start(1);
    }

    private void start(int nbClient) {
        ActorSystem system = ActorSystem.create("LogSystem");

        // Create a client
        for (int i = 0; i < nbClient; i++) {
            ActorRef client1 = stdClient(system);
            // start
            client1.tell("");
        }

    }

    private ActorRef stdClient(ActorSystem system) {
        return system.actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new StdClient(IpGenerator.generateIp(), UserAgent.random(), aal);
            }
        }));
    }

    private ActorRef clientCommingDirectlyOnAProduct(ActorSystem system) {
        return system.actorOf(new Props(new UntypedActorFactory() {
            @Override
            public Actor create() {
                return new DirectClient(IpGenerator.generateIp(), UserAgent.random(), aal);
            }
        }));
    }
}
