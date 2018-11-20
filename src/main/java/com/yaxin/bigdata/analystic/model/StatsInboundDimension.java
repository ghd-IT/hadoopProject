package com.yaxin.bigdata.analystic.model;

import com.yaxin.bigdata.analystic.model.base.BaseDimension;
import com.yaxin.bigdata.analystic.model.base.InboundDimension;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class StatsInboundDimension extends StatsBaseDimension {
    private StatsCommonDimension statsCommonDimension = new StatsCommonDimension();
    private InboundDimension inboundDimension = new InboundDimension();

    public StatsInboundDimension() {
    }

    public StatsInboundDimension(StatsCommonDimension statsCommonDimension, InboundDimension inboundDimension) {
        this.statsCommonDimension = statsCommonDimension;
        this.inboundDimension = inboundDimension;
    }

    @Override
    public void write(DataOutput dataOutput) throws IOException {
        this.statsCommonDimension.write(dataOutput);
        this.inboundDimension.write(dataOutput);
    }


    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.statsCommonDimension.readFields(dataInput);
        this.inboundDimension.readFields(dataInput);
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        StatsInboundDimension other = (StatsInboundDimension) o;
        int tmp = this.statsCommonDimension.compareTo(other.statsCommonDimension);
        if (tmp != 0) {
            return tmp;
        }
        return this.inboundDimension.compareTo(other.inboundDimension);
    }

    public StatsCommonDimension getStatsCommonDimension() {
        return statsCommonDimension;
    }

    public void setStatsCommonDimension(StatsCommonDimension statsCommonDimension) {
        this.statsCommonDimension = statsCommonDimension;
    }

    public InboundDimension getInboundDimension() {
        return inboundDimension;
    }

    public void setInboundDimension(InboundDimension inboundDimension) {
        this.inboundDimension = inboundDimension;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StatsInboundDimension that = (StatsInboundDimension) o;
        return Objects.equals(statsCommonDimension, that.statsCommonDimension) &&
                Objects.equals(inboundDimension, that.inboundDimension);
    }

    @Override
    public int hashCode() {
        return Objects.hash(statsCommonDimension, inboundDimension);
    }

    @Override
    public String toString() {
        return "StatsInboundDimension{" +
                "statsCommonDimension=" + statsCommonDimension +
                ", inboundDimension=" + inboundDimension +
                '}';
    }
}
