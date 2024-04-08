package ${packageName};

import lombok.*;

@Setter
@Getter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class ${className} {

<#list properties as property>
    public ${property.type} ${property.name};
</#list>

    public String getTableName(){
        return "${tableName}";
    }
}