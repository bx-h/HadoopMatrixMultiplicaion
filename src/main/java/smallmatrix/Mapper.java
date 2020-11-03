package smallmatrix;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.conf.*;

import java.io.IOException;
import java.util.HashMap;


/*
* M matrix : (m,n)
* N matrix : (n,p)
*
* 输入：
* M 文件：
*
* | 1 2 |
* | 3 4 |
*
* M i j v
* --------
* M,0,0,1
* M,0,1,2
* M,1,0,3
* M,1,1,4
*
* *********************
*
* N 文件：
*
* | 5 6 |
* | 7 8 |
*
* M j k v
* ---------
* N,0,0,5
* N,0,1,6
* N,1,0,7
* N,1,1,8
*
* */

public class Mapper extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text>{
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        if (indicesAndValue[0].equals("M")) {
            for (int k = 0; k < p; k++) {
                outputKey.set(indicesAndValue[1] + "," + k); // (i, k)
                outputValue.set("M," + indicesAndValue[2] + "," + indicesAndValue[3]); // (M, j, v)
                context.write(outputKey, outputValue);

            }
        }
        else {
            for (int i = 0; i < m; i++) {
                outputKey.set(i + "," + indicesAndValue[2]); // (i, k)
                outputValue.set("N," + indicesAndValue[1] + "," + indicesAndValue[3]); // (N, j, v)
                context.write(outputKey, outputValue);
            }
        }
    }
}
