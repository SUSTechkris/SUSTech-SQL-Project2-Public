package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.UserService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class UserServiceImplement implements UserService
{
    @Override
    public void removeUser(int userId)
    {
        String sql1 = "select * from users where userid = ?";
        String sql2 = "delete from users where userid = ?";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, userId);

            ResultSet resultSet = stmt1.executeQuery();
            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, userId);
            stmt2.executeUpdate();

        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<User> getAllUsers()
    {
        List<User> users = new ArrayList<>();
        String sql1 = "select s.studentId as studentid, s.firstname as firstname, s.lastname as lastname , s.enrolledDate as enrolleddate, " +
                "m.majorId as majorid, m.name as majorname, d.id as departmentid, d.name as departmentname\n" +
                "from student s join major m on m.majorId = s.major join department d on d.id = m.department";
        String sql2 = "select * from instructor";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);)
        {
            ResultSet resultSet1 = stmt1.executeQuery();
            while (resultSet1.next())
            {
                Student student = new Student();
                student.id = resultSet1.getInt("studentid");

                String firstname = resultSet1.getString("firstname");
                String lastname = resultSet1.getString("lastname");
                String fullname = "";
                if((int)firstname.toCharArray()[0] >= 65 && (int)firstname.toCharArray()[0] <= 90
                        ||(int)firstname.toCharArray()[0] >= 97 && (int)firstname.toCharArray()[0] <= 122)
                    fullname = firstname + " " + lastname;
                else
                    fullname = firstname + lastname;

                student.fullName = fullname;
                //student.fullName = resultSet1.getString("fullname");
                student.enrolledDate = resultSet1.getDate("enrolleddate");
                student.major = new Major();
                student.major.id = resultSet1.getInt("majorid");
                student.major.name = resultSet1.getString("majorname");
                student.major.department = new Department();
                student.major.department.id = resultSet1.getInt("departmentid");
                student.major.department.name = resultSet1.getString("departmentname");
                users.add(student);
            }

            ResultSet resultSet2 = stmt2.executeQuery();
            while (resultSet2.next())
            {
                Instructor instructor = new Instructor();
                instructor.id = resultSet2.getInt("instructorid");

                String firstname = resultSet1.getString("firstname");
                String lastname = resultSet1.getString("lastname");
                String fullname = "";
                if((int)firstname.toCharArray()[0] >= 65 && (int)firstname.toCharArray()[0] <= 90
                        ||(int)firstname.toCharArray()[0] >= 97 && (int)firstname.toCharArray()[0] <= 122)
                    fullname = firstname + " " + lastname;
                else
                    fullname = firstname + lastname;

                instructor.fullName = fullname;
                //instructor.fullName = resultSet2.getString("fullname");
                users.add(instructor);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        return users;
    }

    @Override
    public User getUser(int userId)
    {
        String sql1 = "select * from users where userid = ?";
        String sql2 = "select s.studentId as studentid, full_name(s.firstname, s.lastname) as fullname, s.enrolledDate as enrolleddate, " +
                "m.majorId as majorid, m.name as majorname, d.id as departmentid, d.name as departmentname" +
                "from student s join major m on m.majorId = s.major join department d on d.id = m.department studentid = ?";
        String sql3 = "select u.userid, full_name(i.firstname, i.lastname) as fullname " +
                "from users u join instructor i on u.userid = i.instructorid where u.userid = ?";
        User user = new Student();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt3 = connection.prepareStatement(sql3))
        {
            stmt1.setInt(1, userId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();
            int character = resultSet.getInt("character");
            if(character == 0)
            {
                stmt2.setInt(1, userId);
                ResultSet resultSet1 = stmt2.executeQuery();
                Student student = new Student();

                resultSet1.next();

                student.id = resultSet1.getInt("studentid");
                student.fullName = resultSet1.getString("fullname");
                student.enrolledDate = resultSet1.getDate("enrolleddate");
                student.major = new Major();
                student.major.id = resultSet1.getInt("majorid");
                student.major.name = resultSet1.getString("majorname");
                student.major.department = new Department();
                student.major.department.id = resultSet1.getInt("departmentid");
                student.major.department.name = resultSet1.getString("departmentname");
                user = student;
            }
            else if(character == 1)
            {
                stmt3.setInt(1, userId);
                ResultSet resultSet2 = stmt3.executeQuery();
                resultSet2.next();
                Instructor instructor = new Instructor();
                instructor.id = resultSet2.getInt("userid");
                instructor.fullName = resultSet2.getString("fullname");
                user = instructor;
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return user;
    }
}
