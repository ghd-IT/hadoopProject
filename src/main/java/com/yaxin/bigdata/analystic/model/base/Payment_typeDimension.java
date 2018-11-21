package com.yaxin.bigdata.analystic.model.base;

import com.yaxin.bigdata.common.GlobalConstants;
import org.apache.commons.lang.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

public class Payment_typeDimension  extends BaseDimension {
    private int id;
    private String payment_type;

    public Payment_typeDimension() {
    }

    public Payment_typeDimension(String  payment_type) {
        this. payment_type =  payment_type;
    }

    public Payment_typeDimension(int id, String  payment_type) {
        this( payment_type);
        this.id = id;

    }


    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeInt(id);
        dataOutput.writeUTF( payment_type);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readInt();
        this. payment_type = dataInput.readUTF();
    }

    @Override
    public int compareTo(BaseDimension o) {
        if (this == o) {
            return 0;
        }
        Payment_typeDimension other = (Payment_typeDimension) o;
        int tmp = this.id - other.id;
        if (tmp != 0) {
            return tmp;
        }
        return this. payment_type.compareTo(other. payment_type);
    }

    public static Payment_typeDimension getInstance(String  payment_type) {
        if (StringUtils.isEmpty( payment_type)) {
            payment_type = GlobalConstants.DEFAULT_VALUE;

        }
        return new Payment_typeDimension( payment_type);
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getPayment_type() {
        return payment_type;
    }

    public void setPayment_type(String payment_type) {
        this.payment_type = payment_type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Payment_typeDimension that = (Payment_typeDimension) o;
        return id == that.id &&
                Objects.equals(payment_type, that.payment_type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, payment_type);
    }

    @Override
    public String toString() {
        return "Payment_typeDimension{" +
                "id=" + id +
                ", payment_type='" + payment_type + '\'' +
                '}';
    }
}
