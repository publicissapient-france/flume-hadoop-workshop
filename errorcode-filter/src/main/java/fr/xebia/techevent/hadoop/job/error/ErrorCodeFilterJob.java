package fr.xebia.techevent.hadoop.job.error;

public class ErrorCodeFilterJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage : %s [generic options] <error code to search> <input dir> <output dir>\n", ErrorCodeFilterJob.class.getSimpleName());
            return;
        }

        // todo
    }
}
