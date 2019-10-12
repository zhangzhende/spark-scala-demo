package ebrealtimestreamprocessing.java;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @Description 说明类的用途
 * @ClassName ExecuteCallBack
 * @Author zzd
 * @Date 2019/10/9 9:14
 * @Version 1.0
 **/
public interface ExecuteCallBack {
    void resultCallBack(ResultSet resultSet) throws SQLException;
}
