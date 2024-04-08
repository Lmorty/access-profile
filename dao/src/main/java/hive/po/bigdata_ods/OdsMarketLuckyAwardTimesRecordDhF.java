package hive.po.bigdata_ods;

import lombok.*;
import java.math.BigDecimal;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OdsMarketLuckyAwardTimesRecordDhF {

    public Long id;
    public String orderSn;
    public Long userId;
    public Long oldUserGrade;
    public Long newUserGrade;
    public Long recordSource;
    public Long activityId;
    public Long activityType;
    public Long awardRecordId;
    public String userCouponId;
    public String relationOrderSn;
    public Long status;
    public String createTime;
    public String updateTime;
    public Long deleted;
    public Long groupId;
    public String dt;

    public static String getTableName(){
        return "bigdata_ods.ods_market_lucky_award_times_record_dh_f";
    }
}