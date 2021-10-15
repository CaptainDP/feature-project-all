package taichi.utils;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;

/**
 * @ClassName AutoMessage
 * @Description TODO
 * @Author chenhc
 * @Date 2020/09/21 15:26
 **/

public class AutoMessage {
    private static final Logger logger = LoggerFactory.getLogger(AutoMessage.class);

    private static String url = "http://message-center.openapi.corpautohome.com/message/api/v1/message/create";

    public static boolean send(String title, String content, String users, String types) {
        JSONObject object = JSONObject.parseObject("{\"key\":\"5UBRKm0ASkh3JuU\",\"title\":\"测试title\",\"content\":\"测试content\",\"creator\":\"chenhongcai\",\"channel\":[],\"targetEmp\":[]}");
        object.put("title", title);
        object.put("content", content);

        JSONArray arr = new JSONArray();
        for (String type : types.split(",")) {
            switch (type) {
                case "ding":
                    arr.add(100);
                    break;
                case "sms":
                    arr.add(200);
                    break;
                case "email":
                    arr.add(300);
                    break;
                default:
                    arr.add(300);
            }
        }
        object.put("channel", arr);
        String[] split = users.split(",");
        object.put("targetEmp", split);
        try {
            logger.info("报警信息:{}", object.toJSONString());
            post(object.toJSONString());
            return true;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    private static HttpURLConnection getHttpURLConnection(String url) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) ((new URL(url).openConnection()));
        conn.setConnectTimeout(10000);
        conn.setReadTimeout(10000);
        conn.setDoOutput(true);
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setRequestProperty("Accept", "application/json");
        conn.setRequestMethod("POST");
        conn.connect();
        return conn;
    }

    private static void post(String content) throws IOException {
        long start = System.currentTimeMillis();
        HttpURLConnection conn;
        InputStream is = null;
        OutputStream os = null;
        StringBuilder buffer = new StringBuilder();
        try {
            conn = getHttpURLConnection(url);

            byte[] outputBytes = content.getBytes("UTF-8");
            os = conn.getOutputStream();
            os.write(outputBytes);
            os.flush();


            is = conn.getInputStream();
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String tmp;
            while ((tmp = br.readLine()) != null) {
                buffer.append(tmp);
            }
        } finally {
            if (is != null) {
                is.close();
            }
            if (os != null) {
                os.close();
            }
            logger.info("result : {}", buffer);
            if (buffer.toString().endsWith("1}")) {
                logger.debug("Posted(" + (System.currentTimeMillis() - start) + ") : " + content);
            } else {
                logger.warn("Post metrics err(" + (System.currentTimeMillis() - start) + ")  : " + buffer.toString());
            }

        }
    }


    public static void main(String[] args) throws IOException {
        //"title":"向量到Vearch同步【失败】","key":"5UBRKm0ASkh3JuU","content":"向量到Vearch同步【失败】\ncluster:vearch3\ndbName:db_ai_home\nembedding_type:behavior\ntotal:280452\nsuccess:280452\nexpect:300000\n 耗时(s):965"}
        send("向量到Vearch同步【失败】", "向量到Vearch同步【失败】\\ncluster:vearch3\\ndbName:db_ai_home\\nembedding_type:behavior\\ntotal:280452\\nsuccess:280452\\nexpect:300000\\n 耗时(s):965", "13830", "ding,email");
    }
}
