import java.util.Timer;

public class LoadDataToHdfs {
    public static void main(String[] args) {

        /**
         * 每天定时调度一次
         */
        Timer timer = new Timer();

        //每天调度一次删除任务
        timer.schedule(new RemoveTask(), 0,24*60*60*1000);

        //每天启动一次上传任务
        timer.schedule(new CollectDataTask(),0,60*1000);

    }

}
