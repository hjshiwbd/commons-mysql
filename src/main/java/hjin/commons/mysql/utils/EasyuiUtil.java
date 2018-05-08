package hjin.commons.mysql.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import commons.tool.utils.JsonUtil;
import hjin.commons.mysql.mybatis.Pager;

public class EasyuiUtil {
	public static String parseDatagrid(List<?> list) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("total", list.size());
		map.put("rows", list);
		return JsonUtil.toJson(map);
	}

	public static String parseDatagrid(Pager<?> pager) {
		Map<String, Object> map = new HashMap<String, Object>();
		map.put("total", pager.getTotal());
		map.put("rows", pager.getPageList());
		return JsonUtil.toJson(map);
	}
}
