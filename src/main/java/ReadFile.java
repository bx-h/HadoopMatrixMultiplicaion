import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class ReadFile {
    public static void main(String[] args) {
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader("src/main/resources/Ma.txt"));
            String line = reader.readLine();
            for (int i = 0; i < 1; ++i) {
                System.out.println(line);
                line = reader.readLine();
                String[] s = line.split(" ", 3); // [L No value....]
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
