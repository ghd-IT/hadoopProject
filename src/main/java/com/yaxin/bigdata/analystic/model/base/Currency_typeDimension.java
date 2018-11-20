package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.util.Objects;

public class Currency_typeDimension extends  BaseDimension{
    private  int id;
    private String currency_name;

    public Currency_typeDimension() {
    }

    public Currency_typeDimension(String currency_name) {
        this.currency_name = currency_name;
    }

    public Currency_typeDimension(int id, String currency_name) {
        this(currency_name);
        this.id = id;
    }
    public  static  Currency_typeDimension getInstance(String currency_name){
        if(StringUtils.isEmpty(currency_name)){
            currency_name= GlobalConstants.DEFAULT_VALUE;
        }
        return  new Currency_typeDimension(currency_name);
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(currency_name);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id =dataInput.readInt();
        this.currency_name = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if(this==o){
            return 0;
        }
        Currency_typeDimension other = (Currency_typeDimension) o;
        int tmp = this.id -other.id;
        if(tmp!=0){
            return tmp;
        }
        return this.currency_name.compareTo(other.currency_name);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCurrency_name() {
        return currency_name;
    }

    public void setCurrency_name(String currency_name) {
        this.currency_name = currency_name;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Currency_typeDimension that = (Currency_typeDimension) o;
        return id == that.id &&
                Objects.equals(currency_name, that.currency_name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, currency_name);
    }

    @Override
    public String toString() {
        return "Currency_typeDimension{" +
                "id=" + id +
                ", currency_name='" + currency_name + '\'' +
                '}';
    }
}
