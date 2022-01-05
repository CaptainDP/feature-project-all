package taichi.udfs;

import be.cylab.java.roc.Roc;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.util.ArrayList;


/**
 * @program: feature_project
 * @description:
 * @author: chendapeng
 * @create: 2022-01-04 19:49
 **/

public class AucUDF extends UDF {

    public String evaluate(ArrayList<Double> prob, ArrayList<Double> label) {

        double[] ss = new double[prob.size()];
        for (int i = 0; i < prob.size(); i++) {
            ss[i] = prob.get(i);
        }

        int count = 0;
        double[] dd = new double[label.size()];
        for (int i = 0; i < label.size(); i++) {
            dd[i] = label.get(i);
            if (dd[i] == 1) {
                count += 1;
            }
        }
        if (count == 0 || count == label.size()) {
            return "0.0,0," + label.size();
        }

        Roc roc = new Roc(ss, dd);
        return roc.computeAUC() * label.size() + "," + label.size() + "," + label.size();
    }

}
