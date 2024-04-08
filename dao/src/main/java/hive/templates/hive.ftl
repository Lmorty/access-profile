package ${packageName};

import lombok.*;
import java.math.BigDecimal;
import java.util.Date;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ${className} {

<#list properties as property>
    public ${property.type} ${property.name};
</#list>

    public static String getTableName(){
        return "${tableName}";
    }
}