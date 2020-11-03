package bigmatrix;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import sun.rmi.runtime.Log;

import java.io.IOException;
import java.util.Arrays;

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{

    private Logger logger = Logger.getLogger(Mapper.class);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        String[] indicesAndValue = line.split(" ", 3);
        Text outputKey = new Text();
        Text outputValue = new Text();
        Text fakekey = new Text("hello");

        for (int i = 0; i < indicesAndValue.length; ++i) {
            System.out.println(indicesAndValue[i]);
        }
        if (indicesAndValue[0].equals("L")) {
            try {
                for (int k = 1; k <= p; k++) {  // 这个范围内的 k 都要用到这个 i 所在的行
                    outputKey.set(indicesAndValue[1] + "," + k); // (i, k)
                    String[] values = Arrays.copyOfRange(indicesAndValue, 2, indicesAndValue.length);
                    outputValue.set("L," + indicesAndValue[1] + "," + Arrays.toString(values)); // (L, lineno, [....])
                    context.write(outputKey, outputValue);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                logger.error("--------Left--------");
                logger.error(indicesAndValue);
            }
        }
        else {
            try {
                for (int i = 1; i <= m; i++) {
                    outputKey.set(i + "," + indicesAndValue[1]); // (i, k)
                    String[] values = Arrays.copyOfRange(indicesAndValue, 2, indicesAndValue.length);
                    outputValue.set("R," + indicesAndValue[1] + "," + Arrays.toString(values));  // (R, lineno, [....])
                    context.write(outputKey, outputValue);
                }
            } catch (Exception e) {
                logger.error(e.getMessage());
                logger.error("--------Right--------");
                logger.error(indicesAndValue);
            }
        }
    }
}
