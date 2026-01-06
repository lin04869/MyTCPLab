/***************************2.1: ACK/NACK
 **************************** Feng Hong; 2015-12-09*/

package com.ouc.tcp.test;

import com.ouc.tcp.client.TCP_Sender_ADT;
import com.ouc.tcp.client.UDT_RetransTask;
import com.ouc.tcp.client.UDT_Timer;
import com.ouc.tcp.message.*;
import com.ouc.tcp.tool.TCP_TOOL;
import java.util.concurrent.ConcurrentHashMap;

public class TCP_Sender extends TCP_Sender_ADT {

    private TCP_PACKET tcpPack;	//待发送的TCP数据报
    private final int WINDOW_SIZE = 8;
    private final int PAYLOAD_SIZE = 100;
    private int base = 1;
    private int nextSeqNum = 1;
    private UDT_RetransTask reTrans;
    // 缓存已发送的分组
    private ConcurrentHashMap<Integer, TCP_PACKET> sndpkt = new ConcurrentHashMap<>();
    private UDT_Timer timer = new UDT_Timer();
//    // SR为每个分组维护独立的定时器
//    private ConcurrentHashMap<Integer, UDT_Timer> timers = new ConcurrentHashMap<>();
//    // 标记每个包是否已被确认
//    private ConcurrentHashMap<Integer, Boolean> acked = new ConcurrentHashMap<>();

    /*构造函数*/
    public TCP_Sender() {
        super();	//调用超类构造函数
        super.initTCP_Sender(this);		//初始化TCP发送端
    }

    @Override
    public void rdt_send(int dataIndex, int[] appData) {
        // 检查窗口是否已满
        while (nextSeqNum >= base + WINDOW_SIZE * PAYLOAD_SIZE) {
            // 窗口满，等待接收线程通过 waitACK 修改 sendBase
            try {
                Thread.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        //生成TCP数据报（设置序号和数据字段/校验和),注意打包的顺序
        //tcpH.setTh_seq(dataIndex * appData.length + 1);//包序号设置为字节流号：
        // 封装数据包
        tcpH.setTh_seq(nextSeqNum);
        tcpS.setData(appData);
        tcpPack = new TCP_PACKET(tcpH, tcpS, destinAddr);

        tcpPack.getTcpH().setTh_sum((short) 0);
        tcpPack.getTcpH().setTh_sum(CheckSum.computeChkSum(tcpPack));

        try {
            sndpkt.put(nextSeqNum, tcpPack.clone());
//            acked.put(nextSeqNum, false);
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        udt_send(tcpPack);
//        System.out.println("SR Sender: Sent Seq=" + nextSeqNum);
       // 只要窗口内有未确认包，就保证定时器运行
        start_timer();

        // 更新序号
        nextSeqNum += PAYLOAD_SIZE;
    }

    @Override
    //不可靠发送：将打包好的TCP数据报通过不可靠传输信道发送；仅需修改错误标志
    public void udt_send(TCP_PACKET stcpPack) {
        //设置错误控制标志
        tcpH.setTh_eflag((byte)7);//设置为出错/丢包/延时
        //System.out.println("to send: "+stcpPack.getTcpH().getTh_seq());
        //发送数据报
        client.send(stcpPack);
    }

    @Override
    public void waitACK() {
        //累积确认
        while (!ackQueue.isEmpty()) {
            int currentAck = ackQueue.poll();
            //移除所有已确认的报文段
            if ( sndpkt.containsKey(currentAck - PAYLOAD_SIZE) && currentAck> base) {
                for(int i=base;i<currentAck;i+=PAYLOAD_SIZE) {
                    if (sndpkt.containsKey(i)) {
                        sndpkt.remove(i);
                    }
                }
                //窗口左边界base滑动至currentAck
                base=currentAck;
                //无未确认包时，停止全局计时器
                if(sndpkt.isEmpty()) {
                    timer.cancel();
                }
//                // 停止并移除该包的定时器
//                if (timers.containsKey(currentAck)) {
//                    timers.get(currentAck).cancel();
//                    timers.remove(currentAck);
//                }
//                acked.put(currentAck, true);
//
//                // 确认的是窗口左边界，滑动窗口
//                if (currentAck == base) {
//                    while (acked.containsKey(base) && acked.get(base)) {
//                        sndpkt.remove(base);
//                        acked.remove(base);
//                        base += PAYLOAD_SIZE;
//                    }
//                }
            }
        }
    }

    @Override
    public void recv(TCP_PACKET recvPack) {
        // 校验ACK包
        if (CheckSum.computeChkSum(recvPack) == recvPack.getTcpH().getTh_sum()) {
            ackQueue.add(recvPack.getTcpH().getTh_ack());
            //处理ACK报文
            waitACK();
        }
    }

    // 唯一定时器
    private void start_timer() {
        if (timer != null) {
            timer.cancel();
        }
        timer = new UDT_Timer();
        reTrans = new UDT_RetransTask(client, null) {
            @Override
            public void run() {
                if (sndpkt.isEmpty()) {
                    return;
                }
                // TCP超时重传base开始的所有未确认报文段
                for (int i = base; i < nextSeqNum; i += PAYLOAD_SIZE) {
                    TCP_PACKET p = sndpkt.get(i);
                    if (p != null) {
                        udt_send(p);
                    }
                }
                start_timer(); // 重启全局计时器
            }
        };
        timer.schedule(reTrans, 2000, 2000);
    }

}