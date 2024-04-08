package hive.po.bigdata_ods;

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class OdsMarketLuckyAwardDrawRecordDhF {

    public Long id;
    public Long actId;
    public Long userId;
    public Long prizeId;
    public String prizeName;
    public String prizeValue;
    public Long winFlag;
    public Long auditStatus;
    public Long sendStatus;
    public String createTime;
    public Long createUser;
    public String updateTime;
    public Long updateUser;
    public Long deleted;
    public Long awardStatus;
    public Long prizeType;
    public String auditReason;
    public String userName;
    public String userPhone;
    public Long userGrade;
    public String sendReason;
    public Long poolType;
    public String orderSn;
    public Long orderId;
    public String dt;

    public static String getTableName(){
        return "bigdata_ods.ods_market_lucky_award_draw_record_dh_f";
    }
}