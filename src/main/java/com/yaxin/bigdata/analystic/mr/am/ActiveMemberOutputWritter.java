package com.yaxin.bigdata.analystic.mr.am;

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

public class ActiveMemberOutputWritter implements IOutputWritter {
    private  static final Logger logger = Logger.getLogger(ActiveMemberOutputWritter.class);

    @Override
    public void output(Configuration conf, StatsBaseDimension key, StatsOutputValue value, PreparedStatement ps, IDimension iDimension) {
        StatsUserDimension k = (StatsUserDimension) key;
        OutputWritable v = (OutputWritable) value;
        try {
            int activeMember = ((IntWritable) (v.getValue().get(new IntWritable(-1)))).get();
            int i = 0;
            ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getDateDimension()));
            ps.setInt(++i, iDimension.getDimensionIdByObject(k.getStatsCommonDimension().getPlatformDimension()));
            if(v.getKpi().equals(KpiType.BROWSER_ACTIVE_MEMBER)){
                ps.setInt(++i,iDimension.getDimensionIdByObject(k.getBrowserDimension()));
            }
            ps.setInt(++i, activeMember);
            ps.setString(++i, conf.get(GlobalConstants.RUNNING_DATE));//注意这里需要在runner类里面进行赋值
            ps.setInt(++i, activeMember);

            ps.addBatch();//添加到批处理中，批量执行SQL语句
        } catch (Exception e) {
            logger.warn("给ps赋值失败！！！");
        }
    }
}