package hive.po.access_cdm;

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class DwtSellerStudySellerCourseDhF {

    public Long idCode;
    public Long courseId;
    public Integer courseIsCompleted;
    public String dt;

    public static String getTableName(){
        return "access_cdm.dwt_seller_study_seller_course_dh_f";
    }
}