package com.ctwechat.deom;


import java.io.*;
import java.util.Random;

/**
 * 生产10G年龄相关的随机数字
 */
public class CreateBigData {
    private static Random random = new Random();

    public static int generateRandomData(int start, int end) {
        return random.nextInt(end - start + 1) + start;
    }

    /**
     * 产生10G的18-70之间的数据磁盘上
     * 10G = 10 * 1024M = 10 * 1024 * 1024K = 10 * 1024 * 1024 * 1024B
     * 一个整数占4个字节，也就是4B，那么10G数据需要生产 2.684355e9 约等于3e9即 3乘以10的9次方，30亿
     */
    public void generateData(String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        int start = 18;
        int end = 70;
        long startTime = System.currentTimeMillis();
        BufferedWriter bos = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file, true)));
        for (long i = 1; i < Integer.MAX_VALUE * 1.7; i++) {
            StringBuffer data = new StringBuffer("");
            data.append(generateRandomData(start, end));
            if (i % 1000000 != 0) {
                data.append(",");
            }
            bos.write(data.toString());
            // 每100万条记录成一行，100万条数据大概4M
            if (i % 1000000 == 0) {
                bos.write("\n");
            }
        }
        System.out.println("写入完成! 共花费时间:" + (System.currentTimeMillis() - startTime) / 1000 + " s");
        bos.close();
    }
}
