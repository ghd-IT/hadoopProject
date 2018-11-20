package com.yaxin.bigdata.analystic.model.base;
import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class LocationDimension extends BaseDimension {
    private int id;
    private String country;
    private String province;
    private String city;

    public LocationDimension() {
    }

    public LocationDimension(String country, String province, String city) {
        this.country = country;
        this.province = province;
        this.city = city;
    }

    public LocationDimension(int id, String country, String province, String city) {
        this(country, province, city);
        this.id = id;

    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF(country);
        dataOutput.writeUTF(province);
        dataOutput.writeUTF(city);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        id = dataInput.readInt();
        country = dataInput.readUTF();
        province = dataInput.readUTF();
        city = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this ==o){
            return 0;
        }
        LocationDimension other = (LocationDimension) o;
        int tmp = this.id -other.id;
        if (tmp !=0){
            return tmp;
        }
        tmp = this.country.compareTo(other.country);
        if (tmp != 0){
            return tmp;
        }
        tmp =this.province.compareTo(other.province);
        if(tmp !=0){
            return tmp;
        }
        return this.city.compareTo(other.city);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getCountry() {
        return country;
    }

    public void setCountry(String country) {
        this.country = country;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = city;
    }

    @Override
    public String toString() {
        return "LocationDimension{" +
                "id=" + id +
                ", country='" + country + '\'' +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                '}';
    }
    public static LocationDimension getInstance(String country, String province, String city){
        if (StringUtils.isEmpty(country)){
            country=province=city= GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(province)){
            province=city=GlobalConstants.DEFAULT_VALUE;
        }
        if (StringUtils.isEmpty(city)){
            city=GlobalConstants.DEFAULT_VALUE;
        }
        return  new LocationDimension(country,province,city);
    }
}
