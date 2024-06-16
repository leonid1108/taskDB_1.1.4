package jm.task.core.jdbc.dao;
import jm.task.core.jdbc.model.User;
import jm.task.core.jdbc.util.Util;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;


public class UserDaoJDBCImpl implements UserDao {

    private static final String createTableSQL = "CREATE TABLE IF NOT EXISTS UserDB" +
            "( id BIGINT PRIMARY KEY AUTO_INCREMENT NOT NULL," +
            "name VARCHAR(50) NOT NULL," +
            "lastName VARCHAR(50) NOT NULL," +
            "age TINYINT NOT NULL);";
    private static final String droptableSQL = "DROP TABLE IF EXISTS UserDB";
    private static final String saveUserSQL = "INSERT INTO UserDB (name, lastName, age) VALUES (?, ?, ?)";
    private static final  String deleteUserSQL = "DELETE FROM UserDB WHERE id = (?)";
    private static final String readAllUsersSQL = "SELECT * FROM UserDB";
    private static final String cleanTableSQL = "DELETE FROM UserDB";
    public UserDaoJDBCImpl() { }

    @Override
    public void createUsersTable() {
        try ( Connection connection = Util.getConnection();  Statement statement = connection.createStatement();){
            statement.executeUpdate(createTableSQL);
            System.out.println("Таблица успешно создана");
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public void dropUsersTable() {
        try (Connection connection = Util.getConnection(); Statement statement = connection.createStatement();) {
            statement.executeUpdate(droptableSQL);
            System.out.println("Таблица успешно удалена");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void saveUser(String name, String lastName, byte age) {

        try (Connection connection = Util.getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(saveUserSQL);){

            preparedStatement.setString(1, name);
            preparedStatement.setString(2, lastName);
            preparedStatement.setByte(3, age);

            preparedStatement.executeUpdate();

            System.out.println("User с именем — " + name + " добавлен в базу данных");


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void removeUserById(long id) {
        try (Connection connection = Util.getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(deleteUserSQL);) {
            preparedStatement.setLong(1, id);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public List<User> getAllUsers() {
        List<User> users = new ArrayList<>();
        DatabaseMetaData metadata = null;
        try (Connection connection = Util.getConnection(); Statement statement = connection.createStatement();) {
            metadata = connection.getMetaData();
            ResultSet tablesResultSet = metadata.getTables(null, null, "UserDB", null);
            if (tablesResultSet.next()) {
                try (ResultSet resultSet = statement.executeQuery(readAllUsersSQL)) {
                    while (resultSet.next()) {
                        long id = resultSet.getLong("id");
                        String name = resultSet.getString("name");
                        String lastName = resultSet.getString("lastName");
                        byte age = resultSet.getByte("age");

                        User user = new User(name, lastName, age);
                        user.setId(id);
                        users.add(user);
                    }
                }
            } else {
                System.out.println("Таблицы не существует");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return users;
    }

    @Override
    public void cleanUsersTable() {
        try (Connection connection = Util.getConnection(); PreparedStatement preparedStatement = connection.prepareStatement(cleanTableSQL);){
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
