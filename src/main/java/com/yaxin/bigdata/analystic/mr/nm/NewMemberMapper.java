package com.yaxin.bigdata.analystic.mr.nm;


import com.yaxin.bigdata.Util.JdbcUtil;
import com.yaxin.bigdata.Util.MemberUtil;
import com.yaxin.bigdata.analystic.model.StatsCommonDimension;
import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.base.BrowserDimension;
import com.yaxin.bigdata.analystic.model.base.DateDimension;
import com.yaxin.bigdata.analystic.model.base.KpiDimension;
import com.yaxin.bigdata.analystic.model.base.PlatformDimension;
import com.yaxin.bigdata.analystic.model.value.map.TimeOutputValue;
import com.yaxin.bigdata.common.DateEnum;
import com.yaxin.bigdata.common.GlobalConstants;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * 新增会员
 */
public class NewMemberMapper extends Mapper<LongWritable, Text, StatsUserDimension, TimeOutputValue> {
    private static final Logger logger = Logger.getLogger(NewMemberMapper.class);
    private StatsUserDimension k = new StatsUserDimension();
    private TimeOutputValue v = new TimeOutputValue();
    private KpiDimension newMemberKpi = new KpiDimension(KpiType.NEW_MEMBER.kpiName);
    private KpiDimension newBrowserMemberKpi = new KpiDimension(KpiType.BROWSER_NEW_MEMBER.kpiName);
    private Connection conn = null;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        conn = JdbcUtil.getConn();
        //删除当天的会员信息
        MemberUtil.deleteMenberInfoByDate(conf.get(GlobalConstants.RUNNING_DATE), conn);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        if (StringUtils.isEmpty(line)) {
            return;
        }
        //是否是新增会员

        String[] fileds = line.split("\u0001");
        //获取想要的字段
        String serverTime = fileds[1];
        String platform = fileds[13];
        String umid = fileds[4];
        String browserName = fileds[24];
        String browserVersion = fileds[25];
        if (StringUtils.isEmpty(serverTime) || StringUtils.isEmpty(umid)) {
            logger.info("serverTime & uuid is null serverTime:" + serverTime + ".uuid" + umid);
            return;
        }
        if (!MemberUtil.isNewMember(conn, umid)) {
            logger.info("该umid是一个老会员 umid:" + umid);
            return;
        }
        Long stime = Long.valueOf(serverTime);
        PlatformDimension platformDimension = PlatformDimension.getInstance(platform);
        DateDimension dateDimension = DateDimension.buildDate(stime, DateEnum.DAY);
        StatsCommonDimension statsCommonDimension = this.k.getStatsCommonDimension();
        //为 statsCommonlDimension赋值
        statsCommonDimension.setPlatformDimension(platformDimension);
        statsCommonDimension.setDateDimension(dateDimension);
        BrowserDimension browserDimension = new BrowserDimension("", "");
        statsCommonDimension.setKpiDimension(newMemberKpi);
        this.k.setBrowserDimension(browserDimension);
        this.k.setStatsCommonDimension(statsCommonDimension);
        this.v.setId(umid);
        context.write(this.k, this.v);
        statsCommonDimension.setKpiDimension(newBrowserMemberKpi);
        BrowserDimension browserDimension1 = new BrowserDimension(browserName, browserVersion);
        this.k.setBrowserDimension(browserDimension1);
        this.k.setStatsCommonDimension(statsCommonDimension);
        context.write(this.k, this.v);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        JdbcUtil.close(conn, null, null);
    }
}
