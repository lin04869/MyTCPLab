/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.ConcurrentHashMap;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Receiver extends TCP_Receiver_ADT {

    private int expectedSeq = 1;
//    private int lastAck = -1;    // 记录上一个正确ACK的序号
    private final int WINDOW_SIZE = 8;
    // SR缓存失序分组数据
    private ConcurrentHashMap<Integer, int[]> rcvCache = new ConcurrentHashMap<>();

    /*构造函数*/
    public TCP_Receiver() {
        super();	//调用超类构造函数
        super.initTCP_Receiver(this);	//初始化TCP接收端
    }

    @Override
    public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();
        boolean isCorrect = CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum();

        if (isCorrect) {
            // 落在接收窗口内 [expectedSeq, expectedSeq + N*100 - 1]
            if (recvSeq >= expectedSeq && recvSeq < expectedSeq + WINDOW_SIZE * 100) {
                // 设置 ACK 序号并回复
                tcpH.setTh_ack(recvSeq);
                reply(recvPack);

                // 缓存数据
                if (!rcvCache.containsKey(recvSeq)) {
                    rcvCache.put(recvSeq, recvPack.getTcpS().getData());
                }

                // 如果收到的是 expectedSeq，滑动窗口并交付
                if (recvSeq == expectedSeq) {
                    while (rcvCache.containsKey(expectedSeq)) {
                        dataQueue.add(rcvCache.get(expectedSeq));
                        rcvCache.remove(expectedSeq);
                        expectedSeq += 100;
                    }
                }
            }
            // 落在 [expectedSeq - N*100, expectedSeq - 1] 内，属于已确认过的旧包
            else if (recvSeq < expectedSeq && recvSeq >= expectedSeq - WINDOW_SIZE * 100) {
                // 设置 ACK 序号并回复
                tcpH.setTh_ack(recvSeq);
                reply(recvPack);
            }
        }

        // 每20组数据交付一次应用层
        // 收到最后一个包、expectedSeq 到达了文件末尾也交付
        if (dataQueue.size() >= 20 || expectedSeq >= 100001 || recvSeq >= 99901) {
            deliver_data();
        }
    }

    @Override
    public void deliver_data() {
        //检查dataQueue，将数据写入文件
        File fw = new File("recvData.txt");
        BufferedWriter writer;

        try {
            writer = new BufferedWriter(new FileWriter(fw, true));

            //循环检查data队列中是否有新交付数据
            while(!dataQueue.isEmpty()) {
                int[] data = dataQueue.poll();

                //将数据写入文件
                for(int i = 0; i < data.length; i++) {
                    writer.write(data[i] + "\n");
                }

                writer.flush();		//清空输出缓存
            }
            writer.close();
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    @Override
    // 回复ACK报文段
    public void reply(TCP_PACKET replyPack) {
        // 构造 ACK 报文
        // 使用传入的 replyPack（即收到的数据包）的源地址作为目的地
        TCP_PACKET ackPack = new TCP_PACKET(tcpH, tcpS, replyPack.getSourceAddr());
        // 校验和计算
        ackPack.getTcpH().setTh_sum((short) 0);
        ackPack.getTcpH().setTh_sum(CheckSum.computeChkSum(ackPack));

        // 设置错误控制标志
        tcpH.setTh_eflag((byte)0);  // eFlag=0，信道无错误
        //发送数据报
        client.send(ackPack);
    }
}