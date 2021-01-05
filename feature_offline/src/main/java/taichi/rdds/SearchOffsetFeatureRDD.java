package taichi.rdds;

import com.autohome.beans.Segement;
import com.autohome.models.OffsetModel;
import com.autohome.utils.ESHttpClientUtils;
import com.autohome.utils.RedisUtils;
import com.captain.bigdata.taichi.process.transfer.SqlTransfer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Row;
import org.apache.spark.util.LongAccumulator;
import redis.clients.jedis.Jedis;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @ClassName SearchOffsetFeatureRDD
 * @Description TODO
 * @Author chenhc
 * @Date 2021/01/04 17:09
 **/

public class SearchOffsetFeatureRDD extends SqlTransfer {
    @Override
    public void extProcess() {

        final LongAccumulator total = context().session().sparkContext().longAccumulator("total");
        final LongAccumulator success = context().session().sparkContext().longAccumulator("success");
        final LongAccumulator failure = context().session().sparkContext().longAccumulator("failure");
        final LongAccumulator empty = context().session().sparkContext().longAccumulator("empty");

        JavaRDD<Row> rowJavaRDD = super.context().df().javaRDD();

        JavaRDD<Row> offset_result_rdd = rowJavaRDD.mapPartitions(new FlatMapFunction<Iterator<Row>, Row>() {
            @Override
            public Iterator<Row> call(Iterator<Row> rowIterator) throws Exception {
                List<Row> list = new ArrayList<>();
                while(rowIterator.hasNext()){
                    Row next = rowIterator.next();

                    String item_key = next.getAs("item_key");

                    total.add(1);

                    //offset 处理逻辑
                    OffsetModel.Offset offset = offsetToPB(next);
                    if(offset.getSerializedSize()==0) {
                        empty.add(1);
                        continue;
                    }
                    boolean status = pushCodis(item_key.getBytes(), offset.toByteArray());
                    //计数统计
                    if(status) {
                        success.add(1);
                    }else {
                        failure.add(1);
                    }
                }
                return list.iterator();
            }
        }, false);

        RDD<Row> rdd = offset_result_rdd.rdd();

        rdd.collect();

        logger().info("total:{},success:{},failure:{},empty:{}",total.value(),success.value(),failure.value(),empty.value());
    }

    /***
     * 获取offset to pb
     * @param row
     */
    public OffsetModel.Offset offsetToPB(Row row){
        String content = row.getAs("content");
        String author = row.getAs("author");
        String title = row.getAs("title");
        String stitle = row.getAs("stitle");


        List<Segement> contentSegList = ESHttpClientUtils.post(content.length()>1000?content.substring(0,1000):content);
        List<Segement> authorSegList = ESHttpClientUtils.post(author);
        List<Segement> titleSegList = ESHttpClientUtils.post(title);
        List<Segement> stitleSegList = ESHttpClientUtils.post(stitle);

        OffsetModel.Offset offset = ESHttpClientUtils.castPb(titleSegList, stitleSegList, authorSegList, contentSegList);

        return offset;
    }

    /**
     * push to codis
     * @param pb
     * @return
     */
    public boolean pushCodis(byte[] key,byte[] pb){
        Jedis client = RedisUtils.getClient();

        String result = "";
        try {
            result = client.set(key, pb);
        }finally {
            client.close();
            if("OK".equals(result))
                return true;
            return false;
        }
    }
}
