package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.analystic.model.base.DateDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.DateEnum;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 获取时间维度的id
 */
public class DateDimensionUdf extends UDF {
    private IDimension convert = new IDimensionImpl();
    public  int evaluate(String dt){
        if(StringUtils.isEmpty(dt)){
            dt=TimeUtil.getYesterday();
        }
        DateDimension dd = DateDimension.buildDate(TimeUtil.parseString2Long(dt), DateEnum.DAY);
        int id =0;
        try {
            id= convert.getDimensionIdByObject(dd);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  id;
    }

    public static void main(String[] args) {
        System.out.println(new DateDimensionUdf().evaluate("2018-08-30"));
    }
}
