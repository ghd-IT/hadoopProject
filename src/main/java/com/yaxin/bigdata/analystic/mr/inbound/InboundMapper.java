package com.yaxin.bigdata.analystic.mr.inbound;

import com.yaxin.bigdata.analystic.model.StatsCommonDimension;
import com.yaxin.bigdata.analystic.model.StatsInboundDimension;
import com.yaxin.bigdata.analystic.model.base.DateDimension;
import com.yaxin.bigdata.analystic.model.base.InboundDimension;
import com.yaxin.bigdata.analystic.model.base.KpiDimension;
import com.yaxin.bigdata.analystic.model.base.PlatformDimension;
import com.yaxin.bigdata.analystic.model.value.map.InboundOutputValue;
import com.yaxin.bigdata.common.DateEnum;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;


public class InboundMapper extends Mapper<LongWritable, Text, StatsInboundDimension, InboundOutputValue> {
    private static final Logger logger = Logger.getLogger(InboundMapper.class);
    private StatsInboundDimension k = new StatsInboundDimension();
    private InboundOutputValue v = new InboundOutputValue();
    private KpiDimension inboundKpi = new KpiDimension(KpiType.INBOUND.kpiName);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String lines = value.toString();
        if (StringUtils.isEmpty(lines)) {
            return;
        }
        //拆分
        String[] fileds = lines.split("\u0001");
        //获取想要的字段
        String serverTime = fileds[1];
        String platform = fileds[13];
        String uuid = fileds[3];
        String sessionid = fileds[5];
        String url = fileds[11];
        String name = fileds[12];
        if (StringUtils.isEmpty(serverTime)) {
            logger.info("serverTime & uuid is null serverTime:" + serverTime);

            return;
        }
        //构造输出的key
        Long stime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为StatsCommonDimension设
        InboundDimension inboundDimension = InboundDimension.getInstance("0",name,url,"0");
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(inboundKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setInboundDimension(inboundDimension);
        this.v.setUid(uuid);
        this.v.setSid(sessionid);
        context.write(this.k, this.v);
    }
}
