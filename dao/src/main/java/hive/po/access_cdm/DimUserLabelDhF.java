package hive.po.access_cdm;

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DimUserLabelDhF {

    public Long idCode;
    public String userLevel;
    public BigDecimal vValue;
    public String userCreatedDate;
    public Integer isCoAccount;
    public Integer inviterIsCoAccount;
    public Integer blCustIsCoAccount;
    public String userCountry;
    public String userProvince;
    public String userCity;
    public Integer isFillInviteCode;
    public String userLifeCycle;
    public Long blCustIdCode;
    public Long blAsscIdCode;
    public String abmLevel;
    public String sellerCountry;
    public String sellerProvince;
    public String sellerCity;
    public String starLevel;
    public Long custRuCnt;
    public Long custVipCnt;
    public Long custVvipCnt;
    public Long custSvipCnt;
    public Long custHkCnt;
    public Long custUserCnt;
    public Long asscRuCnt;
    public Long asscVipCnt;
    public Long asscVvipCnt;
    public Long asscSvipCnt;
    public Long asscUserCnt;
    public String signSellerContractDate;
    public String currentStarLevelUpgradeDate;
    public BigDecimal actAbmGrowthCntHtd;
    public Long custPrimSharerCnt;
    public Long custMidSharerCnt;
    public Long custSrSharerCnt;
    public Long custSellerCnt;
    public Long asscPrimSharerCnt;
    public Long asscMidSharerCnt;
    public Long asscSrSharerCnt;
    public Integer isOverseasSeller;
    public String lastOrderDateSelf;
    public String lastOrderDateCust;
    public String lastOrderDateAssc;
    public String applyType;
    public String abmApplyType;
    public String brandPreference;
    public String brandCnPreference;
    public String categoryPreference;
    public String lastVisitDateVtn;
    public String lastAddcartDate;
    public String addcartBrand;
    public String addcartBrandCn;
    public Date lastOrderDate;
    public String orderBrand;
    public String orderBrandCn;
    public String lastEffectiveShareIncomeTime;
    public String lastEffectiveAgentIncomeTime;
    public String lastEffectiveMarketProfitTime;
    public Long lastVisitStudyTime;
    public Long lastVisitCustomerDetailTime;
    public String lastVisitGoodsDetailTime;
    public BigDecimal vScore;
    public Integer unusedCouponBrand;
    public Date lastAddcartDateGift3;
    public Date lastAddcartDateGift4;
    public Date lastVisitDateGift3;
    public Date lastVisitDateGift4;
    public String unusedEffctiveCoupons;
    public Integer isNoneCustomsClearanceCertificate;
    public String dt;

    public static String getTableName(){
        return "access_cdm.dim_user_label_dh_f";
    }
}