package com.yaxin.bigdata.analystic.mr.nm;

import com.yaxin.bigdata.analystic.model.StatsUserDimension;
import com.yaxin.bigdata.analystic.model.value.map.TimeOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.common.KpiType;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

public class NewMemberReducer extends Reducer<StatsUserDimension, TimeOutputValue,StatsUserDimension, OutputWritable> {
    private static  final  Logger logger = Logger.getLogger(NewMemberReducer.class);
    private OutputWritable v = new OutputWritable();
    private Set unique = new HashSet<>();
    private MapWritable map = new MapWritable();

    @Override
    protected void reduce(StatsUserDimension key, Iterable<TimeOutputValue> values, Context context) throws IOException, InterruptedException {
        map.clear();
        for (TimeOutputValue tv:values) {
            this.unique.add(tv.getId());
        }
        //新增会员id存入mysql
        for (Object umid: this.unique) {
            map.put(new IntWritable(-2),new Text((String) umid));
            this.v.setValue(map);
            this.v.setKpi(KpiType.MEMBER_INFO);
            context.write(key,this.v);
        }
        this.v.setKpi(KpiType.valueOfKpiName(key.getStatsCommonDimension().getKpiDimension().getKpiName()));
        this.map.put(new IntWritable(-1),new IntWritable(this.unique.size()));
        this.v.setValue(this.map);
        context.write(key,this.v);
        this.unique.clear();
    }
}
