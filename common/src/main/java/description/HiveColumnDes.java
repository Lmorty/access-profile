package description;

/**
 * @author huangxianglei
 * Hive字段描述对象
 */
public class HiveColumnDes {

    /**
     * 字段名
     */
    public String colName;

    /**
     * 字段类型
     */
    public String colType;

    /**
     * 字段注释
     */
    public String colComment;

    public HiveColumnDes() {
    }

    public HiveColumnDes(String colName, String colType, String colComment) {
        this.colName = colName;
        this.colType = colType;
        this.colComment = colComment;
    }

    public String getColName() {
        return colName;
    }

    public void setColName(String colName) {
        this.colName = colName;
    }

    public String getColType() {
        return colType;
    }

    public void setColType(String colType) {
        this.colType = colType;
    }

    public String getColComment() {
        return colComment;
    }

    public void setColComment(String colComment) {
        this.colComment = colComment;
    }
}
