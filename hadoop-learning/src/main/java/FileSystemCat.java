import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

public class FileSystemCat {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        try (InputStream in = fs.open(new Path(args[0]))) {
            IOUtils.copyBytes(in, System.out, 4096, false);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
