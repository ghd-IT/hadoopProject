package com.yaxin.bigdata.analystic.model;

import com.yaxin.bigdata.analystic.model.base.BaseDimension;
import com.yaxin.bigdata.analystic.model.base.BrowserDimension;
import com.yaxin.bigdata.analystic.model.base.LocationDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsLocalDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private LocationDimension locationDimension = new LocationDimension();

    public StatsLocalDimension() {
    }

    public StatsLocalDimension(StatsCommonDimension statsCommonDimension, LocationDimension locationDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.locationDimension = locationDimension;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
            this.statsCommonDimension.write(dataOutput);
            this.locationDimension.write(dataOutput);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommonDimension.readFields(dataInput);
        this.locationDimension.readFields(dataInput);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsLocalDimension other = (StatsLocalDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.locationDimension.compareTo(other.locationDimension);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public LocationDimension getLocationDimension() {
        return locationDimension;
    }

    public void setLocationDimension(LocationDimension locationDimension) {
        this.locationDimension = locationDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsLocalDimension that = (StatsLocalDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(locationDimension, that.locationDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsCommonDimension, locationDimension);
    }

    @Override
    public String toString() {
        return "StatsLocalDimension{" +
                "statsCommonDimension=" + statsCommonDimension +
                ", locationDimension=" + locationDimension +
                '}';
    }
}
