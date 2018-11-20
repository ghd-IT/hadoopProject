package com.yaxin.bigdata.analystic.mr.session;

import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.value.map.TimeOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

public class SessionReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputWritable> {
    private  static final Logger logger = Logger.getLogger(SessionReducer.class);
    private  OutputWritable v = new OutputWritable();
    private Map<String, List<Long>> map= new HashMap<>();
    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
       this.map.clear();
        for (TimeOutputValue tv : values){
           if(this.map.containsKey(tv.getId())){
               this.map.get(tv.getId()).add(tv.getTime());
           }else {
               List<Long> li = new ArrayList<Long>();
               li.add(tv.getTime());
               this.map.put(tv.getId(),li);
           }
        }
        /**
         *  2018-11-02 all 123 list(123321,123145,123423)
         *  2018-11-02 all 125 list(123321,123145,123423)
         */
        //循环时间获取长度
        int sessionLength = 0;
        for (Map.Entry<String,List<Long>> en:map.entrySet()) {
           // if(en.getValue().size() >1 ){
                List<Long> ll = en.getValue();
                sessionLength += Collections.max(ll) - Collections.min(ll);
           // }
        }
        MapWritable map = new MapWritable();

        map.put(new IntWritable(-1),new IntWritable(this.map.size()));
        map.put(new IntWritable(-2),new IntWritable(sessionLength%1000 ==0?sessionLength/1000:sessionLength/1000+1));
        this.v.setValue(map);
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        context.write(key,this.v);

    }
}
