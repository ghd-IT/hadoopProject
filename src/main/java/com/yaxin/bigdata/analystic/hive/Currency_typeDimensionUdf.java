package com.yaxin.bigdata.analystic.hive;

import com.yaxin.bigdata.analystic.model.base.Currency_typeDimension;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.analystic.mr.service.impl.IDimensionImpl;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;
import java.sql.SQLException;

public class Currency_typeDimensionUdf  extends UDF {
 private IDimension convert = new IDimensionImpl();
 public  int evaluate(String currencu_name){
     if(StringUtils.isEmpty(currencu_name)){
        currencu_name= GlobalConstants.DEFAULT_VALUE;
     }
     Currency_typeDimension currency_type = new Currency_typeDimension(currencu_name);
     int id = -1;
     try {
         id =convert.getDimensionIdByObject(currency_type);
     } catch (IOException e) {
         e.printStackTrace();
     } catch (SQLException e) {
         e.printStackTrace();
     }

     return id;
 }

}
