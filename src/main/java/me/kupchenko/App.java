package me.kupchenko;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Hello world!
 */
public class App {

    public App() throws ClassNotFoundException {
        Class.forName("com.mysql.jdbc.Driver");
    }

    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        App app = new App();
        app.select();
        app.selectUsers();
        app.selectUserCount();
        app.insertUser();
        app.updateUser();
        app.deleteUser();
        app.failedTransaction();
        app.fixedTransaction();
        app.innerJoin();
        app.storedProcedure();
    }

    private void select() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("SELECT 1");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                int result = resultSet.getInt(1);
                System.out.println("Result " + result);
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void selectUsers() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("SELECT * FROM users");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                int id = resultSet.getInt("ID");
                int username = resultSet.getInt(2);
                System.out.println("User[" + id + "] " + username);
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void selectUserCount() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("SELECT count(*) `count` FROM users");
            ResultSet resultSet = statement.executeQuery();
            if (resultSet.next()) {
                int id = resultSet.getInt("count");
                System.out.println("Count " + id);
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    // the first parameter is 1, the second is 2, ...
    private void insertUser() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("INSERT INTO users (`username`, `first_name`, `last_name`) VALUES (?, ?, ?)");
            statement.setString(1, "dmitrii");
            statement.setString(2, "Dmitrii");
            statement.setString(3, "Kupchenko");
            int result = statement.executeUpdate();
            System.out.println("Number of inserted rows " + result);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void updateUser() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("UPDATE users SET `first_name`=? WHERE username=?");
            statement.setString(1, "New Dmitrii");
            statement.setString(2, "dmitrii");
            int result = statement.executeUpdate();
            System.out.println("Number of inserted rows " + result);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void deleteUser() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("DELETE FROM users WHERE username=?");
            statement.setString(1, "dmitrii");
            int result = statement.executeUpdate();
            System.out.println("Number of deleted rows " + result);
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void failedTransaction() throws SQLException {
        Connection conn = getConnection();
        try {
            conn.setAutoCommit(false);

            PreparedStatement statement = conn.prepareStatement("INSERT INTO users (`username`, `first_name`, `last_name`) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, "dmitrii");
            statement.setString(2, "Dmitrii");
            statement.setString(3, "Kupchenko");
            int result = statement.executeUpdate();
            if (result <= 0) {
                System.err.println("User is not created");
            }
            ResultSet generatedKeys = statement.getGeneratedKeys();
            int user = -1;
            if (generatedKeys.next()) {
                user = generatedKeys.getInt(1);
            }

            statement = conn.prepareStatement("INSERT INTO addresses (`idddress`, `user`, `address`) VALUES (?, ?, ?)");
            statement.setInt(1, 1);
            statement.setString(2, "Address line here");
            statement.setInt(3, user);

            result = statement.executeUpdate();
            if (result <= 0) {
                System.err.println("Address is not created");
            }
            System.out.println("Number of inserted rows " + result);
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e);
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    private void fixedTransaction() throws SQLException {
        Connection conn = getConnection();
        try {
            conn.setAutoCommit(false);

            PreparedStatement statement = conn.prepareStatement("INSERT INTO users (`username`, `first_name`, `last_name`) VALUES (?, ?, ?)", Statement.RETURN_GENERATED_KEYS);
            statement.setString(1, "dmitrii");
            statement.setString(2, "Dmitrii");
            statement.setString(3, "Kupchenko");
            int result = statement.executeUpdate();
            if (result <= 0) {
                System.err.println("User is not created");
            }
            ResultSet generatedKeys = statement.getGeneratedKeys();
            int user = -1;
            if (generatedKeys.next()) {
                user = generatedKeys.getInt(1);
            }

            statement = conn.prepareStatement("INSERT INTO addresses (`user`, `address`) VALUES (?, ?)");
            statement.setString(1, "Address line here");
            statement.setInt(2, user);

            result = statement.executeUpdate();
            if (result <= 0) {
                System.err.println("Address is not created");
            }
            System.out.println("Number of inserted rows " + result);
            conn.commit();
        } catch (SQLException e) {
            System.out.println(e);
            conn.rollback();
        } finally {
            conn.close();
        }
    }

    private void innerJoin() {
        try (Connection conn = getConnection()) {
            PreparedStatement statement = conn.prepareStatement("SELECT u.username as username, a.address as address FROM `users` u INNER JOIN `addresses` a ON u.id = a.user");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String username = resultSet.getString("username");
                String address = resultSet.getString("address");
                System.out.println("User[" + username + "] with address " + address);
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private void storedProcedure() {
        try (Connection conn = getConnection()) {
            CallableStatement statement = conn.prepareCall("call storedProc()");
            ResultSet resultSet = statement.executeQuery();
            while (resultSet.next()) {
                String username = resultSet.getString("username");
                String address = resultSet.getString("address");
                System.out.println("User[" + username + "] with address " + address);
            }
        } catch (SQLException e) {
            System.out.println(e);
        }
    }

    private Connection getConnection() throws SQLException {
        return DriverManager.getConnection("jdbc:mysql://hostname:port/dbname", "username", "password");
    }
}
