package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.analystic.model.base.Payment_typeDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.SQLException;

public class Payment_typeDimensionUdf  extends UDF {
    private IDimension convert = new IDimensionImpl();
    public  int evaluate(String payment_type){
        if(StringUtils.isEmpty(payment_type)){
            payment_type= GlobalConstants.DEFAULT_VALUE;
        }
        int id = -1;
        Payment_typeDimension payment_typeDimension = new Payment_typeDimension(payment_type);
        try {
            id=convert.getDimensionIdByObject(payment_typeDimension);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return id;
    }
}
