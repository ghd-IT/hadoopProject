package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.analystic.model.base.Event_copyDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

public class Event_copyDimensionUdf extends UDF {
    private IDimension convert = new IDimensionImpl();
    public  int evaluate(String category,String action){
        if(StringUtils.isEmpty(category)){
            category= GlobalConstants.DEFAULT_VALUE;
        }
        if(StringUtils.isEmpty(action)){
            action=GlobalConstants.DEFAULT_VALUE;
        }
        Event_copyDimension event_copyDimension =new Event_copyDimension(category,action);
        int id =0;
        try {
            id= convert.getDimensionIdByObject(event_copyDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  id;
    }

    public static void main(String[] args) {
        System.out.println(new EventDimensionUdf().evaluate("aa","cc"));
    }
}
