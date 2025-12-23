package com.ouc.tcp.test;

import java.util.zip.CRC32;

import com.ouc.tcp.message.TCP_HEADER;
import com.ouc.tcp.message.TCP_PACKET;

public class CheckSum {

    /*计算TCP报文段校验和：只需校验TCP首部中的seq、ack和sum，以及TCP数据字段*/
    public static short computeChkSum(TCP_PACKET tcpPack) {
//		int checkSum = 0;
        CRC32 crc =new CRC32();
        crc.update(tcpPack.getTcpH().getTh_ack());
        crc.update(tcpPack.getTcpH().getTh_seq());
        int data[];
        data=tcpPack.getTcpS().getData();
        for(int e:data){
            crc.update(e);
        }
        return (short) (crc.getValue() & 0xffff); //取低16位
    }

}
