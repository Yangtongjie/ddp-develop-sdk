package cn.com.cloudpioneer.sdk.ddp.develop.spark;

import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SaveMode;

/**
 * 数据显示工具类
 */
public class DataViewUtil {
    /**
     * 保存数据csv
     * @param df DataFrame
     * @param path 原数据输出路径
     */
    public static void saveViewFile(DataFrame df, String path) {
        df.limit(200)
                .repartition(1)
                .write()
                .mode(SaveMode.Overwrite)
                .format("com.databricks.spark.csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .save(path+".ddp.view");
    }
}
