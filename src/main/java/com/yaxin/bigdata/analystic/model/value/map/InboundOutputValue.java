package com.yaxin.bigdata.analystic.model.value.map;

import com.yaxin.bigdata.analystic.model.value.StatsOutputValue;
import com.yaxin.bigdata.common.KpiType;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class InboundOutputValue  extends StatsOutputValue {
    private String uid=""; //对id的泛指，可以是uuid，可以是umid，还可以是sessionId
    private String sid="";

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        if(StringUtils.isNotEmpty(uid)){
            dataOutput.writeUTF(uid);
        }else {
            dataOutput.writeUTF("");
        }
        if(StringUtils.isNotEmpty(sid)){
            dataOutput.writeUTF(sid);
        }else {
            dataOutput.writeUTF("");
        }

    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        uid= dataInput.readUTF();
        sid=dataInput.readUTF();
    }

    @Override
    public KpiType getKpi() {
        return null;
    }

    public String getUid() {
        return uid;
    }

    public void setUid(String uid) {
        this.uid = uid;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }
}

