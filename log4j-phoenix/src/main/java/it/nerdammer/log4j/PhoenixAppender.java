package it.nerdammer.log4j;

import org.apache.log4j.jdbc.JDBCAppender;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Fixed the incompatibilities of Apache Phoenix with the standard log4j JDBC appender.
 *
 * @author Nicola Ferraro
 */
public class PhoenixAppender extends JDBCAppender {

    protected Connection getConnection() throws SQLException {
        Connection connection = super.getConnection();
        connection.setAutoCommit(true);

        return connection;
    }

}
