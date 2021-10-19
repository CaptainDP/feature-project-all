package taichi.utils;

import javax.mail.Address;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @program: feature_project
 * @description: mails
 * @author: chendapeng
 * @create: 2021-10-19 13:45
 **/


public class SendHTMLEmail {


    public static boolean SendMail(String host, String from, List toList, String title, String htmlContent) {

        String to = arrayToString(toList, ",");

        // Recipient's email ID needs to be mentioned.
//        String to = "chendapeng@autohome.com.cn";

        // Sender's email ID needs to be mentioned
//        String from = "chendapeng@autohome.com.cn";

        // Assuming you are sending email from localhost
//        String host = "mail.autohome.com.cn";

        // Get system properties
        Properties properties = System.getProperties();

        // Setup mail server
        properties.setProperty("mail.smtp.host", host);

        // Get the default Session object.
        Session session = Session.getDefaultInstance(properties);

        try {
            // Create a default MimeMessage object.
            MimeMessage message = new MimeMessage(session);

            // Set From: header field of the header.
            message.setFrom(new InternetAddress(from));

            // Set To: header field of the header.
            Address[] internetAddressTo = new InternetAddress().parse(to);
            message.setRecipients(MimeMessage.RecipientType.TO, internetAddressTo);

            // Set Subject: header field
            message.setSubject(title);

            // Send the actual HTML message, as big as you like
            message.setContent(htmlContent, "text/html;charset=utf-8");

            // Send message
            Transport.send(message);
            System.out.println("Sent message successfully....");
        } catch (MessagingException mex) {
            mex.printStackTrace();
            return false;
        }
        return true;
    }

    public static String arrayToString(List strs, String split) {
        if (strs == null || strs.size() == 0) {
            return "";
        } else {
            StringBuilder sbuf = new StringBuilder();
            sbuf.append(strs.get(0));

            for (int idx = 1; idx < strs.size(); ++idx) {
                sbuf.append(split);
                sbuf.append(strs.get(idx));
            }

            return sbuf.toString();
        }
    }

    public static String getDemo(List titleList, List<List> list) {
        StringBuilder content = new StringBuilder("<html><head></head><body>");
        content.append("<table border=\"1\" style=\"width:1000px; height:150px;border:solid 1px #E8F2F9;font-size=11px;font-size:11px;\">");
        String titleTmp = "<tr>";
        for (int i = 0; i < titleList.size(); i++) {
            titleTmp += "<td>" + titleList.get(i) + "</td>";
        }
        titleTmp += "</tr>";
        content.append(titleTmp);

        for (int i = 0; i < list.size(); i++) {
            String tmp = "";
            for (int j = 0; j < list.get(i).size(); j++) {
                tmp += "<td><span>" + list.get(i).get(j) + "</span></td>";
            }
            content.append("<tr>" + tmp + "</tr>");
        }

        content.append("</table>");
        content.append("</body></html>");
        return content.toString();
    }

    public static void main(String[] args) {
        String host = "mail.autohome.com.cn";
        String from = "chendapeng@autohome.com.cn";
        List<String> toList = new ArrayList<String>();
        toList.add("chendapeng@autohome.com.cn");
//        toList.add("zhanglina11592@autohome.com.cn");
//        toList.add("lianshuailong@autohome.com.cn");
//        toList.add("liuyuxing@autohome.com.cn");
//        toList.add("liuyizhuang@autohome.com.cn");
        String title = "北斗实验ctr和时长效果数据";
        String htmlContent = "在原有基础上加薪30%";
        List<List> dataList = new ArrayList<List>();
        List oneList = new ArrayList<>();
        oneList.add("user_perfer_video_score_list");
        oneList.add("2021-10-16");
        oneList.add("12.81%");
        oneList.add("12.64%");
        oneList.add("2.91");
        oneList.add("2.9");
        oneList.add("-0.17%");
        oneList.add("-0.01");
        dataList.add(oneList);
        dataList.add(oneList);
        dataList.add(oneList);
        dataList.add(oneList);

        List titleList = new ArrayList();
        titleList.add("实验");
        titleList.add("日期");
        titleList.add("对照桶ctr");
        titleList.add("实验桶ctr");
        titleList.add("对照桶时长");
        titleList.add("实验桶时长");
        titleList.add("ctr涨幅");
        titleList.add("时长涨幅");

        htmlContent = getDemo(titleList, dataList);
        SendMail(host, from, toList, title, htmlContent);
    }
}
