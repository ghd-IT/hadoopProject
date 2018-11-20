package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class InboundDimension extends BaseDimension {
    private int id;
    private String parent_id;
    private String name;
    private String url;
    private String type;

    public InboundDimension() {
    }

    public InboundDimension(String parent_id, String name, String url, String type) {
        this.parent_id = parent_id;
        this.name = name;
        this.url = url;
        this.type = type;
    }

    public InboundDimension(int id, String parent_id, String name, String url, String type) {
        this(parent_id, name, url, type);
        this.id = id;
    }

    public static InboundDimension getInstance(String parent_id, String name, String url, String type) {
        if (StringUtils.isEmpty(parent_id)) {
            parent_id = name = url = type = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(name)) {
            name = url = type = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(url)) {
            url = type = GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(type)) {
            type = GlobalConstants.DEFAULT_VALUE;
        }
        return new InboundDimension(parent_id,name,url,type);
    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(parent_id);
        dataOutput.writeUTF(name);
        dataOutput.writeUTF(url);
        dataOutput.writeUTF(type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id=dataInput.readInt();
        this.parent_id = dataInput.readUTF();
        this.name =dataInput.readUTF();
        this.url =dataInput.readUTF();
        this.type = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this==o){
            return 0;
        }
        InboundDimension other= (InboundDimension) o;
        int tmp = this.id-other.id;
        if(tmp!=0){
            return tmp;
        }
        tmp = this.parent_id.compareTo(other.parent_id);
        if(tmp!=0){
            return tmp;
        }
        tmp = this.name.compareTo(other.name);
        if(tmp!=0){
            return tmp;
        }
      tmp = this.url.compareTo(other.url);
        if(tmp!=0){
            return tmp;
        }
        return this.type.compareTo(other.type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getParent_id() {
        return parent_id;
    }

    public void setParent_id(String parent_id) {
        this.parent_id = parent_id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InboundDimension that = (InboundDimension) o;
        return id == that.id &&
                Objects.equals(parent_id, that.parent_id) &&
                Objects.equals(name, that.name) &&
                Objects.equals(url, that.url) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, parent_id, name, url, type);
    }

    @Override
    public String toString() {
        return "InboundDimension{" +
                "id=" + id +
                ", parent_id='" + parent_id + '\'' +
                ", name='" + name + '\'' +
                ", url='" + url + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
