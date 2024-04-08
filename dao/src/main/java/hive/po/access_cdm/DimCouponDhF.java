package hive.po.access_cdm;

import lombok.*;
import java.math.BigDecimal;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DimCouponDhF {

    public Long couponId;
    public String couponName;
    public String couponDesc;
    public Integer isCanGiven;
    public String couponIcon;
    public Integer couponStatus;
    public String couponStatusDesc;
    public String couponStartTime;
    public String couponEndTime;
    public Integer couponStock;
    public Integer couponFreezeStock;
    public Integer delayDays;
    public Integer couponUsedCnt;
    public String createdAt;
    public String updatedAt;
    public Integer isDelete;
    public Integer species;
    public String speciesDesc;
    public Integer couponType;
    public String couponTypeDesc;
    public Integer limitNumber;
    public Integer goodsRange;
    public String goodsRangeDesc;
    public Integer goodsTag;
    public String couponRuleCode;
    public String couponRuleData;
    public BigDecimal couponReduceAmt;
    public BigDecimal couponDiscount;
    public BigDecimal couponAfterDiscountAmt;
    public BigDecimal couponDmt;
    public String dt;

    public static String getTableName(){
        return "access_cdm.dim_coupon_dh_f";
    }
}