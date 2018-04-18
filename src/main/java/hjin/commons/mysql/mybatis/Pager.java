package hjin.commons.mysql.mybatis;

import java.io.Serializable;
import java.util.List;

import com.github.miemiedev.mybatis.paginator.domain.PageBounds;

/**
 * 分页类.继承mybatis.RowBounds类,配合mybatis-pager.jar,实现分页功能
 * 
 * @author hjin
 * @cratedate 2013-8-7 上午9:23:13
 */
public class Pager<T> extends PageBounds implements Serializable {

    /**
     * @description
     */
    private static final long serialVersionUID = 1L;
    /**
     * 当前页
     */
    private Integer curtPage = 1;
    /**
     * 每页行数
     */
    private Integer countPerPage = 15;
    /**
     * 总行数
     */
    private Integer total = 0;
    /**
     * 总页数
     */
    private Integer totalPage;

    /**
     * 格式:以","或"."分开,不要有空格.<br/>
     * ex:String orderby = "id.desc,name";
     */
    private String orderby = "";

    /**
     * improved orderby generation with property "orderby"<br/>
     * first "?"->property, second "?"->direction<br/>
     * eg:orderby="id.desc", orderbyFormat="t.??"-> "t.id desc"
     */
    private String orderbyFormat = "";

    /**
     * 数据容器
     */
    private List<T> pageList;
    /**
     * mysql分页参数
     */
    private int offset;
    private int limit;

    /**
     * 默认初始化curtPage=1,countPerPage=15
     */
    public Pager() {
        offset = 1;
        setContainsTotalCount(true);
    }

    /**
     * 计算当前页
     * 
     * @return
     */
    public Integer getCurtPage() {
        if (getTotalPage() > 0 && curtPage > getTotalPage()) {
            curtPage = getTotalPage();
        }
        if (curtPage < 1) {
            curtPage = 1;
        }
        return curtPage;
    }

    public void setCurtPage(Integer curtPage) {
        this.curtPage = curtPage;
    }

    public Integer getCountPerPage() {
        return countPerPage;
    }

    public void setCountPerPage(Integer countPerPage) {
        this.countPerPage = countPerPage;
    }

    public Integer getTotal() {
        return total;
    }

    public void setTotal(Integer total) {
        this.total = total;
    }

    public Integer getTotalPage() {
        totalPage = total % countPerPage == 0 ? total / countPerPage : total / countPerPage + 1;
        return totalPage;
    }

    public void setTotalPage(Integer totalPage) {
        this.totalPage = totalPage;
    }

    public List<T> getPageList() {
        return pageList;
    }

    public void setPageList(List<T> pageList) {
        this.pageList = pageList;
    }

    public String getOrderby() {
        return orderby;
    }

    /**
     * 格式:以","或"."分开,不要有空格.<br/>
     * ex:String orderby = "id.desc,name";
     * 
     * @param orderby
     */
    public void setOrderby(String orderby) {
        this.orderby = orderby;
    }

    @Override
    public int getLimit() {
        limit = countPerPage;
        return limit;
    }

    public void setLimit(int limit) {
        this.limit = limit;
    }

    @Override
    public int getOffset() {
        offset = (curtPage - 1) * countPerPage;
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public String getOrderbyFormat() {
        return orderbyFormat;
    }

    /**
     * {@link #orderbyFormat}
     */
    public void setOrderbyFormat(String orderbyFormat) {
        this.orderbyFormat = orderbyFormat;
    }

}
