package model.Page;

import utils.Utils;

import java.io.IOException;
import java.io.Serializable;

public abstract class AbstractPage implements Serializable {
    private String tableName;
    private int pageIndex; // starts from 0
    private Object min;
    private Object max;
    private int size;

    public AbstractPage(String tableName, int pageIndex) {
        this.tableName = tableName;
        this.pageIndex = pageIndex;
        this.size = 0;
    }

    public String getTableName() {
        return tableName;
    }

    public int getPageIndex() {
        return pageIndex;
    }

    public String getPagePath() {
        return Utils.getPageFilePath(tableName, pageIndex);
    }

    public Object getMin() {
        return min;
    }

    void setMin(Object min) {
        this.min = min;
    }

    public Object getMax() {
        return max;
    }

    void setMax(Object max) {
        this.max = max;
    }

    public int getSize() {
        return size;
    }

    void setSize(int size) {
        this.size = size;
    }

    public boolean isEmpty() {
        return this.size == 0;
    }

    public boolean isFull() throws IOException {
        return this.size == Utils.getMaxRowsCountInPage();
    }

    public boolean isOverflow() throws IOException {
        return this.size > Utils.getMaxRowsCountInPage();
    }

    public int getEmptySpace() throws IOException {
        return Utils.getMaxRowsCountInPage() - getSize();
    }

}
