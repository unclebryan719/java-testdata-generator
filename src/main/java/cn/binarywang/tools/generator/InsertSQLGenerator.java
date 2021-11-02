package cn.binarywang.tools.generator;

import static com.google.common.collect.Collections2.transform;

import java.sql.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.util.*;

import cn.binarywang.tools.generator.util.IdWorker;
import com.apifan.common.random.source.DateTimeSource;
import com.apifan.common.random.source.NumberSource;
import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;

/**
 * @author Binary Wang
 */
public class InsertSQLGenerator {
    private static final Joiner COMMA_JOINER = Joiner.on(", ");
    private Connection con;
    private String tableName;
    private DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String SQL = "SELECT * FROM ";// 数据库操作

    public InsertSQLGenerator(String url, String username, String password,
            String tableName) {
        try {
            Properties props =new Properties();
            props.setProperty("user", username);
            props.setProperty("password", password);
            props.setProperty("remarks", "true"); //设置可以获取remarks信息
            props.setProperty("useInformationSchema", "true");//设置可以获取tables remarks信息
            this.con = DriverManager.getConnection(url, props);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        this.tableName = tableName;
    }

    public String generateSQL() {
        List<String> columnNames = getColumnNames();
        List<String> columnTypes = getColumnTypes();
        ArrayList<String> values = Lists.newArrayListWithCapacity(columnTypes.size());
        for(String columnName : columnNames){
            if(columnName.contains("is_") || columnName.contains("status") || columnName.contains("able") || columnName.contains("flag") || columnName.contains("able")){
                values.add("'"+String.valueOf(NumberSource.getInstance().randomInt(0, 2))+"'");
            }else if(columnName.contains("create_time") || columnName.contains("update_time") || columnName.contains("update_time") || columnName.contains("time")){
                //生成过去365天内的随机时间
                LocalDateTime time = DateTimeSource.getInstance().randomPastTime(365);
                values.add("'"+String.valueOf(time)+"'");
            }else if("id".equals(columnName)){
                values.add("'"+String.valueOf(new IdWorker().nextId())+"'");
            }else{
                values.add("'"+String.valueOf(NumberSource.getInstance().randomInt(1, 101))+"'");
            }
        }
        List<String> tableNames = getTableNames();
        getColumnNames(tableNames.get(0));
        getColumnTypes(tableNames.get(0));
        getColumnComments(tableNames.get(0));
        return String.format("insert into %s(%s) values(%s);", this.tableName,
            COMMA_JOINER.join(columnNames),
            COMMA_JOINER.join(values));
    }

    public String generateParams() {
        return COMMA_JOINER
            .join(transform(getColumnNames(), new Function<String, String>() {

                @Override
                public String apply(String input) {
                    return "abc.get" + CaseFormat.LOWER_UNDERSCORE
                        .to(CaseFormat.UPPER_CAMEL, input) + "()";
                }
            }));
    }

    private List<ColumnVo> getColumnVos() {
        List<ColumnVo> columns = Lists.newArrayList();
        try (PreparedStatement ps = this.con
            .prepareStatement("select * from " + this.tableName);
                ResultSet rs = ps.executeQuery();) {

            ResultSetMetaData rsm = rs.getMetaData();
            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                String columnName = rsm.getColumnName(i);
                String columnType = rsm.getColumnClassName(i);
//                System.out.print("Name: " + columnName);
//                System.out.println(", Type : " + rsm.getColumnClassName(i));
                ColumnVo columnVo = new ColumnVo();
                columnVo.setCloumnName(columnName);
                columnVo.setCloumnType(columnType);
                columns.add(columnVo);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return columns;
    }

    private List<String> getColumnNames() {
        List<String> columns = Lists.newArrayList();
        try (PreparedStatement ps = this.con
            .prepareStatement("select * from " + this.tableName);
             ResultSet rs = ps.executeQuery();) {

            ResultSetMetaData rsm = rs.getMetaData();
            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                String columnName = rsm.getColumnName(i);
//                System.out.print("Name: " + columnName);
                columns.add(columnName);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }

        return columns;
    }


    private List<String> getColumnTypes() {
        List<String> columns = Lists.newArrayList();
        try (PreparedStatement ps = this.con
            .prepareStatement("select * from " + this.tableName);
             ResultSet rs = ps.executeQuery();) {
            ResultSetMetaData rsm = rs.getMetaData();
            for (int i = 1; i <= rsm.getColumnCount(); i++) {
                String columnType = rsm.getColumnClassName(i);
//                System.out.println("Type : " + rsm.getColumnClassName(i));
                columns.add(columnType);
            }

        } catch (SQLException e) {
            e.printStackTrace();
            return null;
        }
        return columns;
    }

    public void a() {
        String sql = "select * from t_c_material";
        PreparedStatement stmt;
        try {
            stmt = con.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery(sql);
            ResultSetMetaData data = rs.getMetaData();
            for (int i = 1; i <= data.getColumnCount(); i++) {
// 获得所有列的数目及实际列数
                int columnCount = data.getColumnCount();
// 获得指定列的列名
                String columnName = data.getColumnName(i);
// 获得指定列的列值
                int columnType = data.getColumnType(i);
// 获得指定列的数据类型名
                String columnTypeName = data.getColumnTypeName(i);
// 所在的Catalog名字
                String catalogName = data.getCatalogName(i);
// 对应数据类型的类
                String columnClassName = data.getColumnClassName(i);
// 在数据库中类型的最大字符个数
                int columnDisplaySize = data.getColumnDisplaySize(i);
// 默认的列的标题
                String columnLabel = data.getColumnLabel(i);
// 获得列的模式
                String schemaName = data.getSchemaName(i);
// 某列类型的精确度(类型的长度)
                int precision = data.getPrecision(i);
// 小数点后的位数
                int scale = data.getScale(i);
// 获取某列对应的表名
                String tableName = data.getTableName(i);
// 是否自动递增
                boolean isAutoInctement = data.isAutoIncrement(i);
// 在数据库中是否为货币型
                boolean isCurrency = data.isCurrency(i);
// 是否为空
                int isNullable = data.isNullable(i);
// 是否为只读
                boolean isReadOnly = data.isReadOnly(i);
// 能否出现在where中
                boolean isSearchable = data.isSearchable(i);
                System.out.println(columnCount);
                System.out.println("获得列" + i + "的字段名称:" + columnName);
                System.out.println("获得列" + i + "的类型,返回SqlType中的编号:"+ columnType);
                System.out.println("获得列" + i + "的数据类型名:" + columnTypeName);
                System.out.println("获得列" + i + "所在的Catalog名字:"+ catalogName);
                System.out.println("获得列" + i + "对应数据类型的类:"+ columnClassName);
                System.out.println("获得列" + i + "在数据库中类型的最大字符个数:"+ columnDisplaySize);
                System.out.println("获得列" + i + "的默认的列的标题:" + columnLabel);
                System.out.println("获得列" + i + "的模式:" + schemaName);
                System.out.println("获得列" + i + "类型的精确度(类型的长度):" + precision);
                System.out.println("获得列" + i + "小数点后的位数:" + scale);
                System.out.println("获得列" + i + "对应的表名:" + tableName);
                System.out.println("获得列" + i + "是否自动递增:" + isAutoInctement);
                System.out.println("获得列" + i + "在数据库中是否为货币型:" + isCurrency);
                System.out.println("获得列" + i + "是否为空:" + isNullable);
                System.out.println("获得列" + i + "是否为只读:" + isReadOnly);
                System.out.println("获得列" + i + "能否出现在where中:"+ isSearchable);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取表中字段的所有注释
     * @param tableName
     * @return
     */
    public List<String> getColumnComments(String tableName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;

        String tableSql = SQL + tableName;
        List<String> columnComments = new ArrayList<>();//列名注释集合
        ResultSet rs = null;
        try {
            pStemt = con.prepareStatement(tableSql);
            rs = pStemt.executeQuery("show full columns from " + tableName);
            while (rs.next()) {
                columnComments.add(rs.getString("Comment"));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println(columnComments);
        return columnComments;
    }

    /**
     * 获取数据库下的所有表名
     */
    public List<String> getTableNames() {
        List<String> tableNames = new ArrayList<>();
        ResultSet rs = null;
        try {
            //获取数据库的元数据
            DatabaseMetaData db = con.getMetaData();
            //从元数据中获取到所有的表名
            rs = db.getTables("sundsoft_dzda", "sundsoft_dzda", null, new String[] { "TABLE" });
            while(rs.next()) {
                tableNames.add(rs.getString(3));
            }
        } catch (SQLException e) {
            System.err.println("getTableNames failure"+e);
        }
        System.out.println(tableNames);
        return tableNames;
    }

    /**
     * 获取表中所有字段名称
     * @param tableName 表名
     * @return
     */
    public List<String> getColumnNames(String tableName) {
        List<String> columnNames = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        try {
            pStemt = con.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnNames.add(rsmd.getColumnName(i + 1));
            }
        } catch (SQLException e) {
            System.err.println("getColumnNames failure"+e);
        }
        System.out.println(columnNames);
        return columnNames;
    }

    /**
     * 获取表中所有字段类型
     * @param tableName
     * @return
     */
    public List<String> getColumnTypes(String tableName) {
        List<String> columnTypes = new ArrayList<>();
        //与数据库的连接
        PreparedStatement pStemt = null;
        String tableSql = SQL + tableName;
        try {
            pStemt = con.prepareStatement(tableSql);
            //结果集元数据
            ResultSetMetaData rsmd = pStemt.getMetaData();
            //表列数
            int size = rsmd.getColumnCount();
            for (int i = 0; i < size; i++) {
                columnTypes.add(rsmd.getColumnTypeName(i + 1));
            }
        } catch (SQLException e) {
            System.err.println("getColumnTypes failure"+e);
        }
        System.out.println(columnTypes);
        return columnTypes;
    }


    public void close() {
        try {
            this.con.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

}
