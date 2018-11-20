package com.yaxin.bigdata.common;

public enum  DateEnum {
    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public  String dateType;

    DateEnum(String dateType){
        this.dateType =dateType;
    }
    public  static DateEnum valueOfDateType(String type){
        for (DateEnum date :values()){
            if(date.dateType.equals(type)){
                return date;
            }
        }
        return null;
    }
}

