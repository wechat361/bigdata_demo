package com.ctwechat.deom;

import java.util.concurrent.LinkedBlockingDeque;

/**
 * 监控
 */
public class MonitorThread implements Runnable{
    /**
     * When an object implementing interface <code>Runnable</code> is used
     * to create a thread, starting the thread causes the object's
     * <code>run</code> method to be called in that separately executing
     * thread.
     * <p>
     * The general contract of the method <code>run</code> is that it may
     * take any action whatsoever.
     *
     * @see Thread#run()
     */
    @Override
    public void run() {
        while (true) {
            try {
                if (DataCenter.isRunning) {
                    System.out.println("监听----文件读取还在进行中，20秒后再试");
                    // 如果读文件的线程还在进行中，则睡眠20秒后再继续监空
                    DataCenter.sleep(30 * 1000);
                    continue;
                }
                StringBuffer stringBuffer = new StringBuffer("");
                for (LinkedBlockingDeque<String[]> blockingDeque : DataCenter.stringQueueList) {
                    stringBuffer.append(blockingDeque.isEmpty());
                }
                if (stringBuffer.toString().contains("false")) {
                    System.out.println("监听----队列中还有数据未处理，20秒后再试");
                    // 如果队列中还有未处理完毕的数据，则睡眠20秒后再继续监空
                    DataCenter.sleep(30 * 1000);
                    continue;
                }
                // 给每一个队列发送一个stop字符串，告知消费者可以停止监听了
                for (LinkedBlockingDeque<String[]> blockingDeque : DataCenter.stringQueueList) {
                    try {
                        blockingDeque.put(new String[]{"stop"});
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                // 结束该监听任务
                System.out.println("监听----数据处理完毕，停止监听");
                break;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
