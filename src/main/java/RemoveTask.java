import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.TimerTask;

public class RemoveTask extends TimerTask {

    public void run() {
        //调用删除过时文件
        try {
            RemoveTask.removeOutdatedFile();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 定时删除文件
     */
    public static void removeOutdatedFile() throws Exception {
        Configuration conf = new Configuration();
        conf.set("dfs.replication", "2");
        conf.set("dfs.blocksize", "128M");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://marshal01:9000/"), conf, "root");

        String deletePath = "/root/hdfsDir/";

        FileStatus[] fileStatuses = fs.listStatus(new Path(deletePath));

        long systemCurrentTime = System.currentTimeMillis();

        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd");
        String sysTime = simpleDateFormat.format(systemCurrentTime);

        for (FileStatus file : fileStatuses) {
            String fileName = file.getPath().getName();
            System.out.println(fileName);
            if (Integer.parseInt(sysTime)-Integer.parseInt(fileName)>=5){
                fs.delete(file.getPath(), true);
            }
        }


    }
}
