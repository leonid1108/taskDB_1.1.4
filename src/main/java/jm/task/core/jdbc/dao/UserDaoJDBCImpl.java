package jm.task.core.jdbc.dao;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class UserDaoJDBCImpl implements UserDao {

    public UserDaoJDBCImpl() { }

    public void createUsersTable() {
        Connection connection = null;
        Statement statement = null;

        try {
            connection = Util.getConnection();
            statement = connection.createStatement();

            String createTableSQL = "CREATE TABLE IF NOT EXISTS User (" +
                    "id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL," +
                    "name VARCHAR(50) NOT NULL," +
                    "lastName VARCHAR(50) NOT NULL," +
                    "age TINYINT NOT NULL);";
            statement.executeUpdate(createTableSQL);
            System.out.println("Таблица успешно создана");
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }

    public void dropUsersTable() {
        Connection connection = null;
        Statement statement = null;

        try {
            connection = Util.getConnection();
            statement = connection.createStatement();

            String sql = "DROP TABLE IF EXISTS User";
            statement.executeUpdate(sql);
            System.out.println("Таблица успешно удалена");
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void saveUser(String name, String lastName, byte age) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;
        ResultSet resultSete = null;
        try {
            connection = Util.getConnection();
            String sql = "INSERT INTO User (name, lastName, age) VALUES (?, ?, ?)";
            preparedStatement = connection.prepareStatement(sql, PreparedStatement.RETURN_GENERATED_KEYS);
            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            int affectedRows = preparedStatement.executeUpdate();

            if (affectedRows > 0) {
                resultSete = preparedStatement.getGeneratedKeys();
                if (resultSete.next()) {
                    long generatedId = resultSete.getLong(1);
                }
            }
            System.out.println("User с именем — " + name + " добавлен в базу данных");


        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public void removeUserById(long id) {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = Util.getConnection();
            String sql = "DELETE FROM User WHERE id = (?)";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        Connection connection = null;
        Statement statement = null;
        ResultSet resultSet = null;
        DatabaseMetaData metadata = null;
        try {
            connection = Util.getConnection();
            statement = connection.createStatement();
            metadata = connection.getMetaData();
            resultSet = metadata.getTables(null, null, "User", null);

            if (resultSet.next()) {
                String sql = "SELECT * FROM User ";
                resultSet = statement.executeQuery(sql);

                while (resultSet.next()) {
                    long id = resultSet.getLong("id");
                    String name = resultSet.getString("name");
                    String lastName = resultSet.getString("lastName");
                    byte age = resultSet.getByte("age");

                    User user = new User(name, lastName, age);
                    user.setId(id);
                    users.add(user);
                }
            } else {
                System.out.println("Таблицы не существует");
            }

        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            try {
                if (resultSet != null) {
                    resultSet.close();
                }
                if (statement != null) {
                    statement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        return users;
    }

    public void cleanUsersTable() {
        Connection connection = null;
        PreparedStatement preparedStatement = null;

        try {
            connection = Util.getConnection();
            String sql = "DELETE FROM User";
            preparedStatement = connection.prepareStatement(sql);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if (preparedStatement != null) {
                    preparedStatement.close();
                }
                if (connection != null) {
                    connection.close();
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
