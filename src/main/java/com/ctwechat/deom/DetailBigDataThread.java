package com.ctwechat.deom;

import java.util.concurrent.CountDownLatch;

/**
 * 处理大数据
 */
public class DetailBigDataThread implements Runnable{
    /**
     * 线程序号，也是list中队列序号
     */
    private int threadNum;
    private CountDownLatch countDownLatch;

    public DetailBigDataThread(int threadNum, CountDownLatch countDownLatch) {
        this.threadNum = threadNum;
        this.countDownLatch = countDownLatch;
    }

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
        System.out.println(threadNum + "------DetailBigDataThread----开启数据监听");
        while (true) {
            try {
                String[] lineData = DataCenter.stringQueueList.get(this.threadNum).take();
                // 当检测到队列中原始含有stop时，表示可以结束监听队列了
                if (lineData[0].contains("stop")) {
                    break;
                }
                DataCenter.detailTotalCount.addAndGet(lineData.length);
                DataCenter.splitStringData1(lineData);
                System.out.println(threadNum + "------DetailBigDataThread----success");
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (this.countDownLatch != null) {
            this.countDownLatch.countDown();
        }
        System.out.println(threadNum + "------DetailBigDataThread----结束数据监听");
    }
}
