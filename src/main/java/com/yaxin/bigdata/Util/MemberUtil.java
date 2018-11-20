package com.yaxin.bigdata.Util;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 判断是否是新增会员的工具类
 */
public class MemberUtil {
    //缓存
    private static Map<String,Boolean> cache = new LinkedHashMap<String, Boolean>(){
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Boolean> eldest) {
            return this.size() > 5000;
        }
    };

    /**
     * 判断是否是会员，如果true 新会员 false 是老会员
     * @param conn
     * @param memberId
     * @return
     */

    public static  boolean isNewMember(Connection conn,String memberId){
        //先查看缓存，如果没有去member_Info查询，如果有就是老会员
        Boolean res = null;
        PreparedStatement ps = null;
        ResultSet rs = null;
        try {
              res = null;
             // if(!cache.containsKey(memberId)){
                res = cache.get(memberId);//空就需要查看数据库
                if(res == null ){
                    ps=conn.prepareStatement("select member_id from member_info where member_id = ?");
                    ps.setString(1,memberId);
                    rs= ps.executeQuery();
                    if(rs.next()){
                        //表示数据库中已经存在该会员
                        res = Boolean.valueOf(false);
                    }else {
                        //新会员
                        res = Boolean.valueOf(true);
                    }
                    //将结果存储在cache
                    cache.put(memberId,res);
                }
            // }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(null,ps,rs);
        }
        return  res == null ? false:res.booleanValue();
    }
    //用于删除指定日期的新增会员的数据
    public  static  void deleteMenberInfoByDate(String date,Connection conn ){
        PreparedStatement ps =null;
        try {
            ps = conn.prepareStatement("delete  FROM member_info where created=?");
            ps.setString(1,date);
            ps.execute();
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            JdbcUtil.close(null,ps,null);
        }
    }
}
