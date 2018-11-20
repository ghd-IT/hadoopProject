package com.yaxin.bigdata.analystic.mr.session;

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
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

public class SessionOutputWritter implements IOutputWritter {
    private static final Logger logger = Logger.getLogger(SessionOutputWritter.class);
    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
            StatsUserDimension k = (StatsUserDimension) key;
            OutputWritable v = (OutputWritable) value;
            try {
            int sessions =( (IntWritable)(v.getValue().get(new IntWritable(-1)))).get();
            int sessionLength=( (IntWritable)(v.getValue().get(new IntWritable(-2)))).get();
            int i=0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            if(v.getKpi().equals(KpiType.BROWSER_SESSION)){
                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
            }
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionLength);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));
            ps.setInt(++i,sessions);
            ps.setInt(++i,sessionLength);
            ps.addBatch();
        } catch (Exception e) {
            logger.warn("赋值ps异常",e);
        }
    }
}
