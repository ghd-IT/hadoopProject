package com.yaxin.bigdata.analystic.mr.local;

import com.yaxin.bigdata.Util.TimeUtil;
import com.yaxin.bigdata.analystic.model.StatsLocalDimension;
import com.yaxin.bigdata.analystic.model.value.map.LocationOutputValue;
import com.yaxin.bigdata.analystic.model.value.reduce.OutputWritable;
import com.yaxin.bigdata.analystic.mr.OutputToMySqlFormat;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.util.Tool;

import java.io.IOException;


public class LocalRunner implements Tool {
    private static final Logger logger = Logger.getLogger(LocalRunner.class);
    private Configuration conf = new Configuration();

    public static void main(String[] args) {
        try {
            ToolRunner.run(new Configuration(), new LocalRunner(), args);
        } catch (Exception e) {
            logger.warn("ACTIVE_USER TO MYSQL is failed !!!", e);
        }
    }

    @Override
    public void setConf(Configuration configuration) {
        conf.addResource("output_mapping.xml");
        conf.addResource("output_writter.xml");
        conf.addResource("other_mapping.xml");
//        conf.addResource("total_mapping.xml");//修改1
        this.conf = conf;
    }

    @Override
    public Configuration getConf() {
        return this.conf;
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        this.setArgs(args, conf);
        Job job = Job.getInstance(conf, "Active");
        job.setJarByClass(LocalRunner.class);
        job.setMapperClass(LocalMapper.class);
        job.setMapOutputKeyClass(StatsLocalDimension.class);
        job.setMapOutputValueClass(LocationOutputValue.class);
        job.setReducerClass(LocalReducer.class);
        job.setOutputKeyClass(StatsLocalDimension.class);
        job.setOutputValueClass(OutputWritable.class);
        job.setOutputFormatClass(OutputToMySqlFormat.class);
        job.setNumReduceTasks(1);
        this.handleInputOutput(job);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void handleInputOutput(Job job) {
        String[] fields = job.getConfiguration().get(GlobalConstants.RUNNING_DATE).split("-");
        String month = fields[1];
        String day = fields[2];

        FileSystem fs = null;
        try {
            fs = FileSystem.get(job.getConfiguration());
            Path inpath = new Path("/ods/" + month + "/" + day);
            if (fs.exists(inpath)) {
                FileInputFormat.addInputPath(job, inpath);
            } else {
                throw new RuntimeException("输入路径不存在inpath" + inpath.toString());
            }
        } catch (IOException e) {
           logger.warn("输入路径异常",e);
        }

    }


    private void setArgs(String[] args, Configuration conf) {
        String date = null;
        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-d")) {
                if (i + 1 < args.length) {
                    date = args[i + 1];
                    break;
                }
            }
        }
        //代码到这儿，date还是null，默认用昨天的时间
        if (date == null) {
            date = TimeUtil.getYesterday();
        }
        //然后将date设置到时间conf中
        conf.set(GlobalConstants.RUNNING_DATE, date);
    }
}
