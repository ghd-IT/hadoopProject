package com.yaxin.bigdata.analystic.mr.inbound;

import com.yaxin.bigdata.analystic.model.StatsBaseDimension;
import com.yaxin.bigdata.analystic.model.StatsInboundDimension;
import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.analystic.model.value.map.InboundOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.analystic.mr.IOutputWritter;
import com.yaxin.bigdata.analystic.mr.service.IDimension;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.log4j.Logger;

import java.sql.PreparedStatement;

public class InboundOutputWritter implements IOutputWritter {
    private  static final Logger logger = Logger.getLogger(InboundOutputValue.class);

    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        StatsInboundDimension k = (StatsInboundDimension) key;
        OutputWritable v= (OutputWritable) value;
        try {
            int uuid =((IntWritable) v.getValue().get(new IntWritable(-1))).get();
            int sessions =((IntWritable) v.getValue().get(new IntWritable(-2))).get();
            int bounce_sessions =((IntWritable) v.getValue().get(new IntWritable(-3))).get();
            int i=0;
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            ps.setInt(++i,iDimension.getDimensionIdByObject(k.getInboundDimension()));
            ps.setInt(++i,uuid);
            ps.setInt(++i,sessions);
            ps.setInt(++i,bounce_sessions);
            ps.setString(++i,conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
            ps.setInt(++i,uuid);
            ps.setInt(++i,sessions);
            ps.setInt(++i,bounce_sessions);
            ps.addBatch();//添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("ps 赋值失败",e);
        }
    }
}

