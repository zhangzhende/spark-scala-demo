package ebusinessInteractivesystem;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Random;

/**
 * @Description 生成测试数据的工具类
 * <p>
 * 格式：用户数据{"userID":0,"name":"name0","registeredTime":"2019-10-01 12:12:36"}
 * 日志数据:{"logID":0,"userID":0,"time":"2019-10-01 12:12:36","typed":0,"consumed":0.0}
 * @ClassName MockData
 * @Author zzd
 * @Date 2019/9/19 9:57
 * @Version 1.0
 **/
public class MockData {
    public static void main(String[] args) {
        long numberTimes = 1000;
        String dataPath = "./data/";
        mockLogData(numberTimes, dataPath);
        mockUserData(numberTimes, dataPath);
    }

    private static void mockLogData(long numberTimes, String dataPath) {
//        {"logID":0,"userID":0,"time":"2019-10-01 12:12:36","typed":0,"consumed":0.0}
        StringBuffer logBuffer = new StringBuffer();
        Random random = new Random();
//        组装数据
        for (int i = 0; i < numberTimes; i++) {
            for (int j = 0; j < numberTimes; j++) {
                String timeprefix = "2019-10-";
                String randomTime = String.format("%s%02d%s%02d%s%02d%s%02d",
                        timeprefix, random.nextInt(31), " ",
                        random.nextInt(24), ":",
                        random.nextInt(60), ":",
                        random.nextInt(60)
                );
                String result = "{\"logID\":" + String.format("%02d", j)
                        + ",\"userID\":" + i + ",\"time\":\"" + randomTime + "\",\""
                        + "typed\":" + String.format("%01d", random.nextInt(2))
                        + ",\"consumed\":" + String.format("%.2f", random.nextDouble() * 1000) + "}";
                logBuffer.append(result).append("\n");
            }
        }
//写数据到文件
        PrintWriter printWriter = null;
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(dataPath + "log.json")));
            printWriter.write(logBuffer.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            printWriter.close();
        }
    }

    private static void mockUserData(long numberTimes, String dataPath) {
//        {"userID":0,"name":"name0","registeredTime":"2019-10-01 12:12:36"}
        StringBuffer logBuffer = new StringBuffer();
        Random random = new Random();
//        组装数据
        for (int i = 0; i < numberTimes; i++) {
            for (int j = 0; j < numberTimes; j++) {
                String timeprefix = "2019-10-";
                String randomTime = String.format("%s%02d%s%02d%s%02d%s%02d",
                        timeprefix, random.nextInt(31), " ",
                        random.nextInt(24), ":",
                        random.nextInt(60), ":",
                        random.nextInt(60)
                );
                String result = "{\"userID\":" + i + ",\"name\":\"name" + i
                        + "\",\"registeredTime\":\"" + randomTime + "\"}";
                logBuffer.append(result).append("\n");
            }
        }
        PrintWriter printWriter = null;
//写数据到文件
        try {
            printWriter = new PrintWriter(new OutputStreamWriter(
                    new FileOutputStream(dataPath + "user.json")));
            printWriter.write(logBuffer.toString());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } finally {
            printWriter.close();
        }
    }


}
