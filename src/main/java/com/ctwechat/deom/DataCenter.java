package com.ctwechat.deom;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DataCenter {
    /**
     * TRUE表示还在继续读取文件，false表示已经结束读取文件，队列监听可以停止了
     */
    public static volatile boolean isRunning;
    /**
     * 消息队列数量，默认是3，可通过初始化来重新赋值
     */
    public static volatile Integer threadNum;
    /**
     * 消息队列初始化数量，从文件中读取出来的字符串根据行号与threadNum取模后作为list下标取出对应的队列进行存放
     */
    public static volatile List<LinkedBlockingDeque<String[]>> stringQueueList;
    //public static volatile List<LinkedBlockingDeque<String>> stringQueueList;
    /**
     * 年龄统计表，key是年龄，value是个数Long型
     */
    public static volatile ConcurrentHashMap<String, AtomicLong> ageCountMap;
    /**
     * 读取时的总记录数
     */
    public static volatile AtomicLong readTotalCount = new AtomicLong(0);
    /**
     * 处理时的总记录数
     */
    public static volatile AtomicLong detailTotalCount = new AtomicLong(0);

    /**
     * 初始化数据中心
     * @param threadNum
     */
    public static void init(Integer threadNum) {
        DataCenter.threadNum = threadNum;
        DataCenter.stringQueueList = new ArrayList<>(DataCenter.threadNum);
        for (int i = 0; i < DataCenter.threadNum; i++) {
            // 10G元素总共3600多行，平均分配给3个队列的话，每个队列里有大概1218行，则容量设计为1280行即可
            DataCenter.stringQueueList.add(new LinkedBlockingDeque<String[]>(1024));
        }
        DataCenter.ageCountMap = new ConcurrentHashMap<>(16);
    }

    /**
     * 获取map中数量最多的年龄对象
     * @return
     */
    public static Map.Entry<String, AtomicLong> getMaxCountAge() {
        long currentAgeCount = 0;
        Map.Entry<String, AtomicLong> maxAgeEntry = null;
        for (Map.Entry<String, AtomicLong> entry : ageCountMap.entrySet()) {
            long ageCount = entry.getValue().get();
            if (ageCount > currentAgeCount) {
                currentAgeCount = ageCount;
                maxAgeEntry = entry;
            }
        }
        return maxAgeEntry;
    }

    /**
     * 获取map中value值的总和
     * @return
     */
    public static Long getCountMapTotalValues() {
        long currentAgeCount = 0;
        for (Map.Entry<String, AtomicLong> entry : ageCountMap.entrySet()) {
            currentAgeCount += entry.getValue().get();
        }
        return currentAgeCount;
    }

    /**
     * 处理从队列中取出来的以逗号分割的年龄字符串，并将其放入到map中
     * @param lineData
     */
    public static void splitStringData1(String lineData) {
        String[] ageArrays = lineData.split(",");
        for (String age : ageArrays) {
            ageCountMap.computeIfAbsent(age, s -> new AtomicLong(0)).getAndIncrement();
        }
    }
    public static void splitStringData1(String[] ageArrays) {
        for (String age : ageArrays) {
            ageCountMap.computeIfAbsent(age, s -> new AtomicLong(0)).getAndIncrement();
        }
    }

    /**
     * 处理从队列中取出来的以逗号分割的年龄字符串，并将其放入到map中，TODO 本实验中下面方法每一上面的方法响应效率高
     * @param lineData
     */
    public static void splitStringData(String lineData) {
        List<String> ageArrays = Stream.of(lineData.split(",")).filter(item->(item != null && !"".equals(item))).collect(Collectors.toList());
        ageArrays.parallelStream().
                forEach(item->ageCountMap.computeIfAbsent(item, s -> new AtomicLong(0)).getAndIncrement());
        /*String[] ageArrays = lineData.split(",");
        int length = ageArrays == null ? 0 : ageArrays.length;
        if (length >= 500000){
            String[] firstArray = Arrays.copyOfRange(ageArrays, 0, 500000);
            String[] secondArray = Arrays.copyOfRange(ageArrays, 500001, length-1);
        }
        ageArrays = null;*/
    }

    public static void printAgeCountMapValue() {
        System.out.println(String.format("当前ageCountMap中元素数据为：" + ageCountMap));
    }

    public static void sleep(long time) {
        try {
            Thread.sleep(time);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
