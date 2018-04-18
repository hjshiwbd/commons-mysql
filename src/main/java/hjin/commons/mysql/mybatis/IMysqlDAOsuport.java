package hjin.commons.mysql.mybatis;

import java.util.List;
import java.util.Map;

public interface IMysqlDAOsuport {
    /**
     * 查询单个对象
     * 
     * @param mybatisId
     * @param param
     * @return
     * 
     */
    public <T> T selectOne(String mybatisId, Object param);

    /**
     * 查询列表
     * 
     * @param mybatisId
     * @param param
     * @return
     * 
     */
    public <T> List<T> selectList(String mybatisId, Object param);

    /**
     * 分页查询.当pager=null时执行selectList
     * 
     * @param mybatisId
     * @param param
     * @param pager
     *            非null时必须初始化curtPage,countPerPage;为null则执行selectList方法.
     * @return
     * 
     */
    public <E> Pager<E> selectPage(String mybatisId, Object param, Pager<E> pager);

    /**
     * 插入对象
     * 
     * @param param
     * @return
     * 
     */
    public int insert(String mybatisId, Object param);

    /**
     * 修改对象
     * 
     * @param mybatisId
     * @param param
     * @return
     * 
     */
    public int update(String mybatisId, Object param);

    /**
     * 删除
     * 
     * @param mybatisId
     * @param param
     * @return
     * 
     */
    public int delete(String mybatisId, Object param);

    /**
     * 查询行数
     * 
     * @param sql
     * @return
     * 
     */
    public int selectCount(String table, String where, Object... param);

    /**
     * 自定义sql查询,返回map对象集合
     * 
     * @param sql
     * @return
     */
    public List<Map<String, Object>> selectDynamicSql(String sql);

    /**
     * 自定义sql查询,返回bean对象list
     * 
     * @param sql
     * @return
     */
    public <T> List<T> selectDynamicSql(String sql, Class<T> cls);

    /**
     * 针对excel生成导出时的查询
     * 
     * @param mybatisId
     * @param param
     * @return
     */
    public List<Map<String, Object>> selectExport(String mybatisId, Object param);
}
