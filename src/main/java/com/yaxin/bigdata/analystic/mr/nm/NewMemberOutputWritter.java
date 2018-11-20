package com.yaxin.bigdata.analystic.mr.nm;

import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.StatsUserDimension;

import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.analystic.mr.IOutputWritter;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.common.GlobalConstants;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.Logger;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class NewMemberOutputWritter implements IOutputWritter {
    private  static  final Logger logger = Logger.getLogger(NewMemberOutputWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        StatsUserDimension k = (StatsUserDimension) key;
        OutputWritable v = (OutputWritable) value;
        switch (v.getKpi()){

            case NEW_MEMBER:
            case BROWSER_NEW_MEMBER:
                try {
                    int i=0;
                    int newMember =((IntWritable) v.getValue().get(new IntWritable(-1))).get();
                    ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
                    ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
                    if(v.getKpi().equals(KpiType.BROWSER_NEW_MEMBER)){
                        ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
                    }
                    ps.setInt(++i,newMember);
                    ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setInt(++i,newMember);
                    ps.addBatch();
                } catch (Exception e) {
                    logger.warn("ps 赋值失败",e);
                }
                break;
            case MEMBER_INFO:
                try {
                    int i =0;
                    Text umid = (Text) v.getValue().get(new IntWritable(-2));
                    ps.setString(++i,umid.toString());
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
                    ps.addBatch();
                } catch (SQLException e) {
                    logger.warn("ps 赋值失败",e);
                }
                break;
                default:
                    throw new RuntimeException("找不到Kpi");
        }

    }
}
