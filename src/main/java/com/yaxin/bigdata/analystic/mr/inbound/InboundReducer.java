package com.yaxin.bigdata.analystic.mr.inbound;

import com.yaxin.bigdata.analystic.model.StatsInboundDimension;
import com.yaxin.bigdata.analystic.model.value.map.InboundOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InboundReducer extends Reducer<StatsInboundDimension, InboundOutputValue,StatsInboundDimension, OutputWritable> {
    private static final Logger logger = Logger.getLogger(InboundReducer.class);
    private OutputWritable v =  new OutputWritable();
    private Set<String> unique = new HashSet();
    private Map<String,Integer> map = new HashMap<>();
    private MapWritable map1 = new MapWritable();

    @Override
    protected void reduce(StatsInboundDimension key, Iterable<InboundOutputValue> values, Context context) throws IOException, InterruptedException {
        map1.clear();
        for (InboundOutputValue lv :values){
            if(StringUtils.isNotEmpty(lv.getUid().trim())){
                this.unique.add(lv.getUid());//添加uuid
            }
            if(StringUtils.isNotEmpty(lv.getUid().trim())){
                if(map.containsKey(lv.getSid())){
                    this.map.put(lv.getSid(),2);//不会跳出会话个数
                }else {
                    this.map.put(lv.getSid(),1);//跳出会话个数
                }
            }
        }
        //构建输出value
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        this.map1.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.map1.put(new IntWritable(-2),new IntWritable(this.map.size()));
        // this.v.setAus(this.unique.size());
        //this.v.setSessions(this.map.size());
        int bounceSession = 0;
        for(Map.Entry<String,Integer> en: map.entrySet()){
            if (en.getValue()==1){
                bounceSession++;
            }
        }
        this.map1.put(new IntWritable(-3),new IntWritable(bounceSession));
        // this.v.setAus(this.unique.size());
        //this.v.setBounce_sessions(bounceSession);
        //设置kpi
        this.v.setValue(this.map1);
        //输出
        context.write(key,this.v);
        this.unique.clear();
    }
}
