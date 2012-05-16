package fr.xebia.techevent.flume.gen;

import java.util.Random;

public class IpGenerator {

    private static final Random r = new Random();

    private static int firstPart() {
        // Return 5, 31, 37, 46, 80, 128
        int[] firstPart = {5, 31, 37, 46, 80, 128};
        return firstPart[r.nextInt(5)];
    }

    private static int otherPart() {
        // return number between 1 and 254
        return r.nextInt(254) + 1;
    }

    public static String generateIp(){
        StringBuilder sb = new StringBuilder();
        sb.append(firstPart());
        sb.append(".");
        sb.append(otherPart());
        sb.append(".");
        sb.append(otherPart());
        sb.append(".");
        sb.append(otherPart());
        return sb.toString();
    }

}
