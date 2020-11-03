package smallmatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.fs.Path;

import javax.xml.soap.Text;
import java.io.IOException;

public class MatrixMultiply {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
//        if (args.length != 2) {
//            System.err.println("Usage: smallmatrix.MatrixMultiply <in_dir> <out_dir>");
//            System.exit(2);
//        }
        Configuration conf = new Configuration();
        conf.set("m", "1000");
        conf.set("n", "100");
        conf.set("p", "1000");
        @SuppressWarnings("deprecation")
                Job job = new Job(conf, "smallmatrix.MatrixMultiply");
        job.setJarByClass(MatrixMultiply.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setMapperClass(Mapper.class);
        job.setReducerClass(Reducer.class);
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        FileInputFormat.addInputPath(job, new Path("src/main/resources/Ma.txt"));
        FileOutputFormat.setOutputPath(job, new Path("src/main/resources/Mb.txt"));

        job.waitForCompletion(true);

    }
}
