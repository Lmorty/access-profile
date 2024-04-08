package hive.po.access_cdm;

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DimAwardActivityDhF {

    public Long actId;
    public String actName;
    public String actStartTime;
    public String actEndTime;
    public String drawStartTime;
    public String drawEndTime;
    public Long drawType;
    public String drawTypeDesc;
    public Long actStatus;
    public String actStatusDesc;
    public Long awardSendType;
    public String awardSendDesc;
    public String actRule;
    public Long isShareFlag;
    public String sharePoster;
    public String shareTitle;
    public String shareDesc;
    public String winAwardRule;
    public String createdAt;
    public Long createdUser;
    public String updatedAt;
    public Long updatedUser;
    public Long isDeleted;
    public String dt;

    public static String getTableName(){
        return "access_cdm.dim_award_activity_dh_f";
    }
}