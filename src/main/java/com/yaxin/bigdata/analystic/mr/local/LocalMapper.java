package com.yaxin.bigdata.analystic.mr.local;


import com.yaxin.bigdata.analystic.model.StatsCommonDimension;
import com.yaxin.bigdata.analystic.model.StatsLocalDimension;
import com.yaxin.bigdata.analystic.model.base.*;
import com.yaxin.bigdata.analystic.model.value.map.LocationOutputValue;
import com.yaxin.bigdata.analystic.mr.nu.NewUserMapper;
import com.yaxin.bigdata.common.DateEnum;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;

public class LocalMapper extends Mapper<LongWritable, Text, StatsLocalDimension, LocationOutputValue> {
    private static final Logger logger = Logger.getLogger(NewUserMapper.class);
    private StatsLocalDimension k = new StatsLocalDimension();
    private LocationOutputValue v = new LocationOutputValue();
    private KpiDimension localKpi = new KpiDimension(KpiType.LOCAL.kpiName);

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
        String country = fileds[28];
        String province = fileds[29];
        String city = fileds[30];
        if (StringUtils.isEmpty(serverTime)) {
            logger.info("serverTime & uuid is null serverTime:" + serverTime);

            return;
        }
        //构造输出的key
        Long stime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        LocationDimension locationDimension = LocationDimension.getInstance(country, province, city);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为StatsCommonDimension设
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setDateDimension(dateDimension);
        statsCommonDimension.setKpiDimension(localKpi);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.k.setLocationDimension(locationDimension);
        this.v.setUid(uuid);
        this.v.setSid(sessionid);
        context.write(this.k, this.v);
    }

}
