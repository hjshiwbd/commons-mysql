package hjin.commons.mysql.mybatis.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;

import hjin.commons.mysql.mybatis.IMysqlDAO;
import hjin.commons.mysql.mybatis.IMysqlDAOsuport;
import hjin.commons.mysql.mybatis.Pager;

public abstract class MysqlDAO implements IMysqlDAO {
    @Autowired
    @Qualifier("daoSupport")
    protected IMysqlDAOsuport mysqlDAOsuport;

    public void setMysqlDAOsuport(IMysqlDAOsuport mysqlDAOsuport) {
        this.mysqlDAOsuport = mysqlDAOsuport;
    }

    @Override
    public int insert(Object param) {
        return mysqlDAOsuport.insert(getNamespace() + "insert", param);
    }

    @Override
    public int delete(Object param) {
        return mysqlDAOsuport.delete(getNamespace() + "delete", param);
    }

    @Override
    public int update(Object param) {
        return mysqlDAOsuport.update(getNamespace() + "update", param);
    }

    @Override
    public <E> List<E> selectList(Object param) {
        return mysqlDAOsuport.selectList(getNamespace() + "select", param);
    }

    @Override
    public <T> T selectOne(Object param) {
        return mysqlDAOsuport.selectOne(getNamespace() + "select", param);
    }

    @Override
    public <E> Pager<E> selectPage(Object param, Pager<E> pager) {
        return mysqlDAOsuport.selectPage(getNamespace() + "select", param, pager);
    }

    @Override
    public int selectCount(String table, String where, Object... param) {
        return mysqlDAOsuport.selectCount(table, where, param);
    }

}
