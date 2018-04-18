package hjin.commons.mysql.mybatis;

import java.util.List;

public interface IMysqlDAO {

    /**
     * mapper命名空间的前缀
     * 
     * @return
     * 
     */
    public String getNamespace();

    /**
     * 保存对象
     * 
     * @param param
     * @return
     * 
     */
    public int insert(Object param);

    /**
     * 删除对象
     * 
     * @param param
     * @return
     * 
     */
    public int delete(Object param);

    /**
     * 修改对象
     * 
     * @param param
     * @return
     * 
     */
    public int update(Object param);

    /**
     * 列表查询
     * 
     * @param param
     * @return
     * 
     */
    public <E> List<E> selectList(Object param);

    /**
     * 单对象查询
     * 
     * @param param
     * @return
     * 
     */
    public <T> T selectOne(Object param);

    /**
     * 分页查询
     * 
     * @param param
     * @param pager
     * @return
     * 
     */
    public <E> Pager<E> selectPage(Object param, Pager<E> pager);

    /**
     * 行数查询
     * 
     * @param table
     * @param where
     * @param param
     * @return
     */
    public int selectCount(String table, String where, Object... param);

}
