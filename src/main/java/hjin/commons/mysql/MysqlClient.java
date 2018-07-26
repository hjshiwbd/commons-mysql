package hjin.commons.mysql;

import commons.tool.utils.JsonUtil;
import org.apache.commons.dbutils.DbUtils;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ArrayListHandler;
import org.apache.commons.dbutils.handlers.BeanListHandler;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 通过connection完成数据库操作
 */
public class MysqlClient {
    private Logger logger = LoggerFactory.getLogger(getClass());

    private Connection conn;

    public MysqlClient(Connection conn) {
        this.conn = conn;
    }

    /**
     * update
     *
     * @param sql
     * @param param
     * @return
     */
    public int update(String sql, Object... param) {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(param));
        }
        QueryRunner runner = new QueryRunner();
        try {
            return runner.update(conn, sql, param);
        } catch (SQLException e) {
            logger.error("", e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return 0;
    }

    /**
     * insert
     *
     * @param sql
     * @param param
     * @return
     */
    public int insert(String sql, Object... param) {
        logger.debug("insert start");
        return update(sql, param);
    }

    /**
     * delete
     *
     * @param sql
     * @param param
     * @return
     */
    public int delete(String sql, Object... param) {
        logger.debug("delete start");
        return update(sql, param);
    }

    /**
     * selectOne
     *
     * @param sql
     * @param param
     * @return
     */
    public Map<String, Object> selectOne(String sql, Object... param) {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(param));
        }
        QueryRunner runner = new QueryRunner();
        try {
            return runner.query(conn, sql, new MapHandler(), param);
        } catch (SQLException e) {
            logger.error("", e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return null;
    }

    /**
     * map list,返回的map的key均为大写
     *
     * @param sql
     * @param param
     * @return
     */
    public List<Map<String, Object>> selectList(String sql, Object... param) {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(param));
        }
        QueryRunner runner = new QueryRunner();
        try {
            return runner.query(conn, sql, new MapListHandler(), param);
        } catch (SQLException e) {
            logger.error("", e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return new ArrayList<>();
    }

    /**
     * bean list
     *
     * @param sql
     * @param clz
     * @param params
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> List<T> selectBeanList(String sql, Class<T> clz, Object... params) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(params));
        }
        List<T> list;
        QueryRunner runner = new QueryRunner();
        try {
            list = runner.query(conn, sql, new BeanListHandler<>(clz), params);
        } catch (SQLException e) {
            logger.error("", e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return list;
    }

    /**
     * bean list
     *
     * @param sql
     * @param clz
     * @param params
     * @param <T>
     * @return
     * @throws Exception
     */
    public <T> T selectBeanOne(String sql, Class<T> clz, Object... params) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(params));
        }

        List<T> list = selectBeanList(sql, clz, params);
        if (list.isEmpty()) {
            return null;
        } else {
            if (list.size() == 1) {
                return list.get(0);
            } else {
                throw new RuntimeException("expected 1 row but found " + list.size());
            }
        }
    }

    /**
     * mysql insert并且返回自增id
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public int[] insertReturnId(String sql, Object... params) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(params));
        }
        QueryRunner runner = new QueryRunner();
        int[] result = {0, 0};
        try {
            int n = runner.update(conn, sql, params);
            String idsql = "select LAST_INSERT_ID() LASTINSERTID";
            logger.info(idsql);
            Number lastid = (Number) runner.query(idsql, new MapHandler()).get("LASTINSERTID");
            logger.info("lastid:" + lastid);
            result[0] = n;
            result[1] = lastid.intValue();
        } catch (Exception e) {
            logger.error("", e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
        return result;
    }

    /**
     * 查询行数
     *
     * @param sql
     * @param params
     * @return
     * @throws Exception
     */
    public long selectCount(String sql, Object... params) throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("sql:{}", sql);
            logger.debug("param:{}", JsonUtil.toJson(params));
        }
        QueryRunner runner = new QueryRunner();
        List<Object[]> list;
        try {
            list = runner.query(conn, sql, new ArrayListHandler(), params);
            if (list != null && list.size() == 1) {
                Object o = list.get(0)[0];
                if (o instanceof Number) {
                    return ((Number) o).longValue();
                } else {
                    throw new RuntimeException("result is not a number");
                }
            } else {
                throw new RuntimeException("result rows > 1");
            }
        } catch (SQLException e) {
            logger.error("", e);
            throw new RuntimeException(e);
        } finally {
            DbUtils.closeQuietly(conn);
        }
    }


    public void setConn(Connection conn) {
        this.conn = conn;
    }

}
