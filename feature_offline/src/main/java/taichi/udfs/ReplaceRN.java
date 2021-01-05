package taichi.udfs;

import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * ReplaceRN
 *
 * @author <a href=mailto:captain_cc_2008@163.com>CaptainDP</a>
 * @date 2018/8/18 10:40
 * @func
 */
public class ReplaceRN extends UDF {


    /**
     * 替换
     *
     * @param content
     * @return
     */
    public String evaluate(String content) {

        String newString = content;
        Pattern CRLF = Pattern.compile("(\r\n|\r|\n|\n\r)");
        Matcher m = CRLF.matcher(content);
        if (m.find()) {
            newString = m.replaceAll("");
        }

        return newString;
    }

    public static void main(String[] content) {
        ReplaceRN udf = new ReplaceRN();
        String a = udf.evaluate("sdfs\rfdsfsdf\n,dsfsdfsdf\r\n");
        System.out.println(a);

    }
}

