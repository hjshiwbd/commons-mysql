package hjin.commons.mysql;

import com.alibaba.fastjson.JSON;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.MapHandler;
import org.apache.commons.dbutils.handlers.MapListHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleMysqlClient {
	private Logger logger = LoggerFactory.getLogger(getClass());

	private DataSource dataSource;
	private Boolean pmdKnownBroken = false;

	public SimpleMysqlClient(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	/**
	 * update
	 * 
	 * @param sql
	 * @param param
	 * @return
	 */
	public int update(String sql, Object... param) {
		QueryRunner runner = new QueryRunner(dataSource, pmdKnownBroken);
		try {
			return runner.update(sql, param);
		} catch (SQLException e) {
			logger.error("", e);
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
		QueryRunner runner = new QueryRunner(dataSource, pmdKnownBroken);
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("sql:{}", sql);
				logger.debug("param:{}", JSON.toJSONString(param));
			}
			return runner.query(sql, new MapHandler(), param);
		} catch (SQLException e) {
			logger.error("", e);
		}
		return null;
	}

	/**
	 * selectList
	 * 
	 * @param sql
	 * @param param
	 * @return
	 */
	public List<Map<String, Object>> selectList(String sql, Object... param) {
		QueryRunner runner = new QueryRunner(dataSource, pmdKnownBroken);
		try {
			if (logger.isDebugEnabled()) {
				logger.debug("sql:{}", sql);
				logger.debug("param:{}", JSON.toJSONString(param));
			}
			return runner.query(sql, new MapListHandler(), param);
		} catch (SQLException e) {
			logger.error("", e);
		}
		return new ArrayList<>();
	}

	public void setDataSource(DataSource dataSource) {
		this.dataSource = dataSource;
	}

	public void setPmdKnownBroken(Boolean pmdKnownBroken) {
		this.pmdKnownBroken = pmdKnownBroken;
	}
}
