package com.quickstart.model;

import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

@Getter
@Setter
@ToString
public class TableMeta {
    private String name;
    private String type;
    /**
     * 表所属数据库
     */
    private String cat;
    /**
     * 表所属用户名
     */
    private String userName;
    /**
     * 表备注
     */
    private String remark;

    private ColumnMeta pkCol;
    private ColumnMeta partitionCol;

    private List<ColumnMeta> columnMetaList = new ArrayList<>();

    public void addColMeta(ColumnMeta cm) {
        this.columnMetaList.add(cm);
    }

}
