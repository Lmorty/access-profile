package hive.po.access_cdm;

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DwtSellerStudySellerPackageDhF {

    public Long idCode;
    public Long packageId;
    public Integer packageIsCompleted;
    public String dt;

    public static String getTableName(){
        return "access_cdm.dwt_seller_study_seller_package_dh_f";
    }
}