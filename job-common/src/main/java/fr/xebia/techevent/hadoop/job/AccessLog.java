package fr.xebia.techevent.hadoop.job;

public class AccessLog {

    public final String ip;
    public final String timestamp;
    public final String command;
    public final String resources;
    public final String protocol;
    public final String returnCode;
    public final String processTime;

    public AccessLog(String ip, String timestamp, String command, String resources, String protocol, String returnCode, String processTime) {
        this.ip = ip;
        this.timestamp = timestamp;
        this.command = command;
        this.resources = resources;
        this.protocol = protocol;
        this.returnCode = returnCode;
        this.processTime = processTime;
    }
}
