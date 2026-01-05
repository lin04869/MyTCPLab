/***************************2.1: ACK/NACK*****************/
/***** Feng Hong; 2015-12-09******************************/
package com.ouc.tcp.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import com.ouc.tcp.client.TCP_Receiver_ADT;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;

public class TCP_Receiver extends TCP_Receiver_ADT {

    private TCP_PACKET ackPack;	//回复的ACK报文段
    private int expectedSeq = 1; // 期望序号，与发送端同步
    private int lastAck = -1;    // 记录上一个正确ACK的序号

    /*构造函数*/
    public TCP_Receiver() {
        super();	//调用超类构造函数
        super.initTCP_Receiver(this);	//初始化TCP接收端
    }

    @Override
    public void rdt_recv(TCP_PACKET recvPack) {
        int recvSeq = recvPack.getTcpH().getTh_seq();
        boolean isCorrect = CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum();

        if (isCorrect && recvSeq == expectedSeq) {
            // 成功按序接收
            System.out.println("Receiver: Accept Seq=" + recvSeq);
            dataQueue.add(recvPack.getTcpS().getData());

            lastAck = recvSeq;
            expectedSeq += 100;
        } else {

        }

        // 构造并发送ACK（无论对错都发ACK，回复最后一次正确的ACK）
        tcpH.setTh_ack(lastAck);
        ackPack = new TCP_PACKET(tcpH, tcpS, recvPack.getSourceAddr());
        // 先置0，再计算
        ackPack.getTcpH().setTh_sum((short) 0);
        ackPack.getTcpH().setTh_sum(CheckSum.computeChkSum(ackPack));

        reply(ackPack);

        // 每20组数据交付一次应用层
        if (dataQueue.size() >= 20) {
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
    //回复ACK报文段
    public void reply(TCP_PACKET replyPack) {
        //设置错误控制标志
        tcpH.setTh_eflag((byte)0);	//eFlag=0，信道无错误

        //发送数据报
        client.send(replyPack);
    }
}