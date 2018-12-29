import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.File;
import java.net.URI;
import java.util.TimerTask;

public class CollectDataTask extends TimerTask {
    /*
    srcDir ：源数目录
    destDir ：目标数据目录
    upDir ：数据待上传目录
     */
    File srcDir = new File("/root/hdfsDir/");
    File destDir = new File("hdfs://marshal01:9000/");

    public void run() {
        //调用上传新文件
        try {
            CollectDataTask.loadFile2Hdfs(srcDir, destDir);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 定时上传文件
     */
    public static void loadFile2Hdfs(File srcDir, File destDir) throws Exception {

        Configuration conf = new Configuration();
        conf.set("dfs.replication", "3");
        conf.set("dfs.blocksize", "128M");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fs = FileSystem.get(new URI("hdfs://marshal01:9000/"), conf, "root");

        File[] files = srcDir.listFiles();
        //遍历目录下的文件
        for (File file : files) {
            //如果是目录回调方法 file=/home/didiOrgPath/20180301/
            if (file.isDirectory()){
                File absoluteFile = file.getAbsoluteFile();//absoluteFile=/home/didiOrgPath/20180301/

                CollectDataTask.loadFile2Hdfs(absoluteFile,new File(destDir+file.getName()));
            }else{
                //如果是文件上传到目的地
                fs.moveFromLocalFile(new Path(file.getAbsolutePath()), new Path("hdfs://marshal01:9000/"+file.getAbsolutePath()));
            }
        }


    }

}
