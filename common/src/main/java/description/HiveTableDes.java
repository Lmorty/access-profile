package description;

import java.util.List;
import java.util.Map;

/**
 * @author huangxianglei
 * Hive表描述对象
 */
public class HiveTableDes {

    /**
     * 库名
     */
    public String dbName;

    /**
     * 表名
     */
    public String tableName;


    /**
     * 完整表名称:库.表
     */
    private String fullTableName;

    /**
     * 表备注
     */
    public String tableComment;


    /**
     * 表字段描述(不包含分区字段)
     */
    public List<HiveColumnDes> colsDescription;

    /**
     * 分区字段描述
     */
    public List<HiveColumnDes> partColsDescription;

    public HiveTableDes() {
    }

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public HiveTableDes(String fullTableName, String tableComment, List<HiveColumnDes> colsDescription, List<HiveColumnDes> partColsDescription) {
        this.fullTableName = fullTableName;
        this.tableComment = tableComment;
        this.colsDescription = colsDescription;
        this.partColsDescription = partColsDescription;

        this.dbName = fullTableName.split("\\.")[0];
        this.tableName = fullTableName.split("\\.")[1];
    }

    public HiveTableDes(String dbName, String tableName, String tableComment, List<HiveColumnDes> colsDescription, List<HiveColumnDes> partColsDescription) {
        this.dbName = dbName;
        this.tableName = tableName;
        this.tableComment = tableComment;
        this.colsDescription = colsDescription;
        this.partColsDescription = partColsDescription;
        this.fullTableName = dbName + "." + tableName;
    }

    public String getFullTableName() {
        return fullTableName;
    }

    public void setFullTableName(String fullTableName) {
        this.fullTableName = fullTableName;
    }

    public String getTableComment() {
        return tableComment;
    }

    public void setTableComment(String tableComment) {
        this.tableComment = tableComment;
    }

    public List<HiveColumnDes> getColsDescription() {
        return colsDescription;
    }

    public void setColsDescription(List<HiveColumnDes> colsDescription) {
        this.colsDescription = colsDescription;
    }

    public List<HiveColumnDes> getPartColsDescription() {
        return partColsDescription;
    }

    public void setPartColsDescription(List<HiveColumnDes> partColsDescription) {
        this.partColsDescription = partColsDescription;
    }
}
