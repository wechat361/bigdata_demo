package com.ctwechat.deom;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;

/**
 * 单线程读取数据
 */
public class ReadBigDataFromFile {

    /**
     * 单线程处理文件中的数据
     * @param filePath
     * @throws IOException
     */
    public void readData(String filePath) throws IOException {
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
        // 开始时间
        long startTime = System.currentTimeMillis();
        long startTime2 = startTime;
        int count = 1;
        String line;
        while ((line = bufferedReader.readLine()) != null) {
            /*System.out.println("第"+count+"行：" + line);*/
            if (count % 100 == 0) {
                System.out.println("读取100行,总耗时间: " + (System.currentTimeMillis() - startTime) / 1000 + " s");
                DataCenter.sleep(1000);
                System.gc();
                startTime = System.currentTimeMillis();
            }
            DataCenter.splitStringData1(line);
            count++;
        }
        bufferedReader.close();
        long endTime = System.currentTimeMillis();
        System.out.println("总共读取读取"+count+"行,总耗时间: " + (endTime - startTime2) / 1000 + " s");
        //DataCenter.printAgeCountMapValue();
    }

    /**
     * 利用消息队列，将文件中的数据分散到多个队列中，然后由多个消费者来共同处理该数据
     * @param filePath
     * @param threadNum
     * @throws IOException
     */
    public void readData(String filePath, int threadNum) {
        String line;
        int count = 1;
        int baiCount = 0;
        long endTime = 0L;
        long startTime = 0L;
        long startTime2 = 0L;
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath));
            // 开始时间
            startTime = System.currentTimeMillis();
            startTime2 = startTime;
            while ((line = bufferedReader.readLine()) != null) {
                try {
                    // 根据处理字符串的线程数量将字符串进行拆分
                    this.splitLineData(line, threadNum);
                    //DataCenter.stringQueueList.get(count % threadNum).put(line);
                    if (count % 100 != 0) {
                        //DataCenter.sleep(1);
                        continue;
                    }
                    System.out.println("读取第"+(baiCount + 1)+"个100行, 此次消耗时间: " + (System.currentTimeMillis() - startTime) / 1000 + " s");
                    startTime = System.currentTimeMillis();
                    // 睡眠几秒后，让系统回收下垃圾
                    //DataCenter.sleep(500);
                    //System.gc();
                    baiCount++;
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } finally {
                    count++;
                }
            }
            bufferedReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        endTime = System.currentTimeMillis();
        System.out.println("总共读取读取"+(count - 1)+"行,总耗时间: " + (endTime - startTime2) / 1000 + " s");
    }

    /**
     * 根据处理字符串的线程数量将字符串进行拆分
     * @param lineData 从文件中读取到的字符串
     * @param threadNum 当前系统启动的用于从队列中读取字符串的线程数
     * @throws InterruptedException
     */
    public void splitLineData(String lineData, int threadNum) throws InterruptedException {
        // 将字符串根据行号取模后，存入队列中
        String[] split = lineData.split(",");
        // 统计读取到的年龄总数
        DataCenter.readTotalCount.addAndGet(split.length);
        //对字符串分段，每段显示的元素个数
        int length = split.length / threadNum;
        for (int i = 0; i < threadNum; i++) {
            // Arrays.copyOfRange中的第3个参数值
            int toIndex;
            if (i < threadNum - 1) {
                toIndex = (i + 1) * length;
            } else {
                toIndex = split.length;
            }
            // 从split数组中拷贝数据，包含from，不包含to
            String[] tempStrArray = Arrays.copyOfRange(split, i * length, toIndex);
            //System.out.println("split str "+i+"=============length = " + tempStrArray.length + " === orgi array length = " + split.length);
            DataCenter.stringQueueList.get(i).put(tempStrArray);
            //String newString = String.join(",", tempStrArray);
            //DataCenter.stringQueueList.get(i).put(newString);
        }
    }

}
