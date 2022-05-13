package com.ctwechat.deom;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demo主入口
 */
public class BigDataMainApp {
    // 第1个参数表示执行哪个操作，第二个参数表示消费者线程数
    public static void main(String[] args) {
        String cmd = "100";
        int threadNum = 2;
        String filepath = "/home/bigdata.txt";
        // 指定执行哪个操作
        if (args != null && args.length > 0) {
            cmd = args[0];
        }
        // 指定处理字符串时使用的线程数
        if (args != null && args.length > 1) {
            threadNum = Integer.parseInt(args[1]);
        }
        // 指定bigdata.txt的路径，默认/home目录
        if (args != null && args.length > 2) {
            filepath = args[2] + (args[2].endsWith("/") ? "" : "/") + "bigdata.txt";
        }
        DataCenter.init(threadNum);
        switch (cmd) {
            case "0":// 生产10G数据
                createBigData(filepath);
                break;
            case "1":// 单线程读取并处理字符串
                singleDetailBigData(filepath);
                break;
            case "2":// 一个线程读文件，多个线程处理字符串
                multipDetailBigData(filepath, threadNum);
                System.out.println("read ages count = " + DataCenter.readTotalCount.get());
                System.out.println("detail ages count = " + DataCenter.detailTotalCount.get());
                break;
            default:
                System.out.println("has no thread is running ... ");
                break;
        }
        System.out.println("统计数据：" + DataCenter.ageCountMap);
        System.out.println("集合中数据总和为：" + DataCenter.getCountMapTotalValues());
        // 获取最大的年龄对象
        Map.Entry<String, AtomicLong> maxCountAge = DataCenter.getMaxCountAge();
        if (maxCountAge != null) {
            System.out.println("数量最多的年龄为:" + maxCountAge.getKey() + "数量为：" + maxCountAge.getValue());
        }
    }

    private static void createBigData(String filepath) {
        try {
            new CreateBigData().generateData(filepath);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void singleDetailBigData(String filepath) {
        try {
            new ReadBigDataFromFile().readData(filepath);
            /*Map.Entry<String, AtomicLong> maxCountAge = DataCenter.getMaxCountAge();
            if (maxCountAge != null) {
                System.out.println("数量最多的年龄为:" + maxCountAge.getKey() + "数量为：" + maxCountAge.getValue());
            }*/
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void multipDetailBigData(String filepath, int threadNum) {
        try {
            // 开启多线程共同处理队列中的数据
            CountDownLatch countDownLatch = new CountDownLatch(DataCenter.threadNum);
            ExecutorService executorService = Executors.newFixedThreadPool(DataCenter.threadNum + 1);
            for (int i = 0; i < DataCenter.threadNum; i++) {
                executorService.execute(new DetailBigDataThread(i, countDownLatch));
            }
            // 开启队列监听
            executorService.execute(new MonitorThread());
            // 将文件中的数据根据行号取模后存入多个消息队列中
            DataCenter.isRunning = true;
            new ReadBigDataFromFile().readData(filepath, threadNum);
            DataCenter.isRunning = false;
            // 等待多线程执行完毕
            countDownLatch.await();
            System.out.println("*************************************");
            // 关闭线程池
            executorService.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
