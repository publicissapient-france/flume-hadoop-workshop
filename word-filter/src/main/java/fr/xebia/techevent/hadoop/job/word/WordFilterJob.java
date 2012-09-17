package fr.xebia.techevent.hadoop.job.word;

public class WordFilterJob {

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.out.printf("Usage : %s [generic options] <word to be filtered> <input dir> <output dir>\n", WordFilterJob.class.getSimpleName());
            return;
        }

        // todo
    }
}
