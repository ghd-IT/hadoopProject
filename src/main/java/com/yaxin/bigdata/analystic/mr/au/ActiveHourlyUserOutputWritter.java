//package com.yaxin.bigdata.analystic.mr.au;
//
//import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
//import com.yaxin.bigdata.analystic.model.StatsUserDimension;
//import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
//import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
//import com.yaxin.bigdata.analystic.mr.IOutputWritter;
//import com.yaxin.bigdata.analystic.mr.nu.BrowserNewUserOutputWritter;
//import com.yaxin.bigdata.analystic.mr.service.IDimension;
//import com.yaxin.bigdata.common.GlobalConstants;
//import com.yaxin.bigdata.common.KpiType;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.io.IntWritable;
//import org.apache.log4j.Logger;
//
//import java.sql.PreparedStatement;
//
//public class ActiveHourlyUserOutputWritter implements IOutputWritter {
//    private static final Logger logger = Logger.getLogger(ActiveHourlyUserOutputWritter.class);
//    @Override
//    //这里通过key和value给ps语句赋值
//    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
//        try {
//            StatsUserDimension k = (StatsUserDimension) key;
//            OutputWritable v = (OutputWritable) value;
//
//            //获取新增用户的值
//            //int activeUser = ((IntWritable)(v.getValue().get(new IntWritable(-1)))).get();
//
//            int i = 0;
//            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
//            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
//            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getKpiDimension()));
//           for(i++;i<28;i++){
//               int activeUser = ((IntWritable)v.getValue().get(new IntWritable(i-4))).get();
//               System.out.println(activeUser);
//               ps.setInt(i,activeUser);
//                ps.setInt(i+25,activeUser);
//           }
////            for(int j=0;j<24;j++){
////                int activeUser = ((IntWritable)v.getValue().get(new IntWritable(j))).get();
////                ps.setInt(j+4,activeUser);
////            }
//            ps.setString(i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
////            for(int j=0;j<24;j++){
////                int activeUser = ((IntWritable)v.getValue().get(new IntWritable(j))).get();
////                ps.setInt(j+29,activeUser);
////            }
//            ps.addBatch();//添加到批处理中，批量执行SQL语句
//        } catch (Exception e) {
//            logger.warn("给ps赋值失败！！！");
//        }
//    }
//}