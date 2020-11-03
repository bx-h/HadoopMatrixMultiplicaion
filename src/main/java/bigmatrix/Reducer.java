package bigmatrix;

import org.apache.hadoop.io.Text;

import javax.xml.bind.DatatypeConverter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.regex.Pattern;

import static javax.xml.bind.DatatypeConverter.parseHexBinary;

public class Reducer extends org.apache.hadoop.mapreduce.Reducer<Text, Text, Text, Text>{
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String[] value;
        HashMap<String, String> hashA = new HashMap<String, String>();
        HashMap<String, String> hashB = new HashMap<String, String>();
        for (Text val : values) {
            value = val.toString().split(",");
            if (value[0].equals("L")) { // hashA: (i,k),[]
                hashA.put(key.toString(), value[2]);
            }
            else {                      // hashB: (i,k),[]
                hashB.put(key.toString(),value[2]);
            }
        }
        int result = 0;
        if (hashA.containsKey(key.toString()) && hashB.containsKey(key.toString())) {
            String Row = hashA.get(key.toString());
            String Column = hashB.get(key.toString());
            String[] row = Row.replaceAll("\\[", "").replaceAll("\\]", "").split(" ");
            String[] column = Column.replaceAll("\\[", "").replaceAll("\\]", "").split(" ");

            for (int i = 0; i < row.length; ++i) {
                byte[] bytes = parseHexBinary(row[i]);
                byte[] bytes1 = parseHexBinary(column[i]);
                ByteBuffer wrapped = ByteBuffer.wrap(bytes); // big-endian by default
                ByteBuffer wrapped1 = ByteBuffer.wrap(bytes1);
                int r = wrapped.getInt();
                int c = wrapped1.getInt();

                result += r * c;
            }
        }
        if (result != 0) {
            System.out.printf("%d\n", result);
            // key 应该是 (i, k)
            context.write(null, new Text(key.toString() + "," + Float.toString(result)));
        }
    }
}
