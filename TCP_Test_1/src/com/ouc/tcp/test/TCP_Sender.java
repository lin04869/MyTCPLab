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
    private final int WINDOW_SIZE = 8;      // GBN窗口大小
    private final int PAYLOAD_SIZE = 100;    // 序号增量
    private int base = 1;               // 窗口左边界
    private int nextSeqNum = 1;             // 下一个待发序号
    private UDT_RetransTask reTrans;
    // 缓存已发送但未ACK的包，使用线程安全的Map
    private ConcurrentHashMap<Integer, TCP_PACKET> sndpkt = new ConcurrentHashMap<>();
    private UDT_Timer timer = new UDT_Timer();

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

        // 存入缓存并发送
        try {
            sndpkt.put(nextSeqNum, tcpPack.clone());
        } catch (CloneNotSupportedException e) {
            e.printStackTrace();
        }

        udt_send(tcpPack);
        System.out.println("Sender: Sent Seq=" + nextSeqNum + " (Base=" + base + ")");

        // 窗口第一个包，启动定时器
        if (base == nextSeqNum) {
            startTimer();
        }

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
        // 处理所有积压的有效ACK
        while (!ackQueue.isEmpty()) {
            int currentAck = ackQueue.poll();
            //System.out.println("CurrentAck: "+currentAck);
            // 累积确认：收到ACK n表示 n及n以前的包都收到了
            if (currentAck >= base) {
                System.out.println("Sender: Received ACK=" + currentAck);
                // 清理已确认的包
                for (int i = base; i <= currentAck; i += PAYLOAD_SIZE) {
                    sndpkt.remove(i);
                }
                // 滑动窗口
                base = currentAck + PAYLOAD_SIZE;
                // 定时器管理
                if (base == nextSeqNum) {
                    timer.cancel(); // 全部包已确认，停表
                } else {
                    startTimer();   // 还有包没确认，为最早的包重开计时器
                }
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
        } else {
            System.out.println("Sender: Received Corrupt ACK!");
        }

    }

    private void startTimer() {
        if (timer != null) timer.cancel();
        timer = new UDT_Timer();

        reTrans = new UDT_RetransTask(client, null) {
            @Override
            public void run() {
                System.out.println("Sender: Timeout! Go-Back-N Resending from=" + base);
                // 超时重传整个窗口内所有已发包
                for (int i = base; i < nextSeqNum; i += PAYLOAD_SIZE) {
                    TCP_PACKET p = sndpkt.get(i);
                    if (p != null) client.send(p);
                }
                startTimer(); // 重传后重新计时
            }
        };
        timer.schedule(reTrans, 2000, 2000); // 2秒超时
    }

}