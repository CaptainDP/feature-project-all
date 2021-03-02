package com.autohome.utils;

import com.autohome.models.OffsetModel;
import com.google.protobuf.InvalidProtocolBufferException;
import com.googlecode.protobuf.format.JsonFormat;
import redis.clients.jedis.Jedis;

/**
 * @ClassName GetOffsetPB
 * @Description TODO
 * @Author chenhc
 * @Date 2021/02/03 11:17
 **/

public class GetOffsetPB {
    public static void main(String[] args) throws InvalidProtocolBufferException {
        String host = args[0];
        int port = Integer.parseInt(args[1]);
        String passwd = args[2];
        String key = args[3];

        Jedis jedis = new Jedis(host, port);
        jedis.auth(passwd);
        OffsetModel.Offset offset = OffsetModel.Offset.parseFrom(jedis.get(key.getBytes()));
        System.out.println(JsonFormat.printToString(offset));
        jedis.close();
    }
}
