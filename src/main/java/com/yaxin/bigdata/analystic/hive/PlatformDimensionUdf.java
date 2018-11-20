package com.yaxin.bigdata.analystic.hive;


import com.yaxin.bigdata.analystic.model.base.PlatformDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

/**
 * 获取platform维度的id
 */
public class PlatformDimensionUdf extends UDF {
    private IDimension convert = new IDimensionImpl();
    public  int evaluate(String platform){
        if(StringUtils.isEmpty(platform)){
            platform= GlobalConstants.DEFAULT_VALUE;
        }
        PlatformDimension platformDimension = new PlatformDimension(platform);
        int id =-1;
        try {
            id= convert.getDimensionIdByObject(platformDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return  id;
    }

    public static void main(String[] args) {
        System.out.println(new PlatformDimensionUdf().evaluate("website"));
    }
}
