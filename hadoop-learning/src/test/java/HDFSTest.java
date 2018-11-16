import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

public class HDFSTest {

    public static final String HADOOP_USER_NAME = "cuibowen";

    private static Configuration conf;
    private static FileSystem fs;

    @BeforeClass
    public static void beforeClass() throws IOException {
        conf = new Configuration();
        fs = FileSystem.get(conf);
    }

    @Test
    public void testCreateFolder() throws IOException {
        String path = "/user/" + HADOOP_USER_NAME;
        Assert.assertTrue(fs.mkdirs(new Path(path)));
    }

    @Test
    public void testCopyFile() throws IOException {
        String localPath = System.getProperty("user.dir");//获取当前工程目录
        String path = "/user/" + HADOOP_USER_NAME + "/input";
        fs.copyFromLocalFile(new Path(localPath), new Path(path));
        Assert.assertTrue(fs.exists(new Path(path + "/pom.xml")));
    }

    @Test
    @Ignore
    public void testView() throws IOException {
        RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("input/src"), true);
        while (iterator.hasNext()) {
            LocatedFileStatus locatedFileStatus = iterator.next();
            System.out.println(locatedFileStatus);
        }
    }

    @Test
    @Ignore
    public void testDeleteFolder() throws IOException {
        String path = "/user";
        Assert.assertTrue(fs.delete(new Path(path), true));
    }
}
