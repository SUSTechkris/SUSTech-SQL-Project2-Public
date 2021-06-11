package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.MajorService;
import io.netty.channel.epoll.EpollTcpInfo;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class MajorServiceImplement implements MajorService
{
    @Override
    public int addMajor(String name, int departmentId)
    {
        String sql1 = "select m.majorid from major m where m.name = ? and m.department = ?;";
        String sql2 = "insert into major (name, department) values (?,?);";
        String sql3 = "select m.majorid as majorid from major m where m.name = ? and m.department = ?";
        int number = 0;
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt3 = connection.prepareStatement(sql3))
        {
            stmt1.setString(1, name);
            stmt1.setInt(2, departmentId);

            ResultSet resultSet = stmt1.executeQuery();
            if(resultSet.next())
                throw new IntegrityViolationException();

            stmt2.setString(1, name);
            stmt2.setInt(2, departmentId);
            stmt2.executeUpdate();

            stmt3.setString(1, name);
            stmt3.setInt(2, departmentId);
            ResultSet resultSet1 = stmt3.executeQuery();
            resultSet1.next();
            number = resultSet1.getInt("majorid");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return number;
    }

    @Override
    public void removeMajor(int majorId)
    {
        String sql1 = "select m.name from major m where m.majorid = ?";
        String sql2 = "delete from major where majorid = ?";
        int number = 0;
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, majorId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, majorId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public List<Major> getAllMajors()
    {
        List<Major> majors = new ArrayList<>();
        String sql = "select m.majorid as majorid, m.name as majorname, d.id as departmentid,\n" +
                "d.name as departmentname from major m join department d on m.department = d.id;";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt.executeQuery();
            int count = 0;
            while (resultSet.next())
            {
                count++;
                Major major = new Major();
                major.id = resultSet.getInt("majorid");
                major.name = resultSet.getString("majorname");
                major.department = new Department();
                major.department.id = resultSet.getInt("departmentid");
                major.department.name = resultSet.getString("departmentname");
                majors.add(major);
            }
            if(count == 0)
                throw new EntityNotFoundException();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return majors;
    }

    @Override
    public Major getMajor(int majorId)
    {
        String sql = "select m.majorid as majorid, m.name as majorname, d.id as departmentid," +
                "d.name as departmentname from major m join department d on m.department = d.id" +
                "where m.majorid = ?;";
        Major major = new Major();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt.executeQuery();
            major.id = resultSet.getInt("majorid");
            major.name = resultSet.getString("majorname");
            major.department = new Department();
            major.department.id = resultSet.getInt("departmentid");
            major.department.name = resultSet.getString("departmentname");
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return major;
    }

    @Override
    public void addMajorCompulsoryCourse(int majorId, String courseId)
    {
        String sql1 = "select coursetypeinmajor from course_with_major cm where cm.majorid = ? and cm.courseid = ?";
        String sql2 = "insert into course_with_major (courseid, majorid, coursetypeinmajor) values (?,?,?)";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, majorId);
            stmt1.setString(2, courseId);
            ResultSet resultSet = stmt1.executeQuery();
            if(resultSet.next())
                throw new IntegrityViolationException();

            stmt2.setString(1, courseId);
            stmt2.setInt(2, majorId);
            stmt2.setInt(3, 0);
            //0代表必修课
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void addMajorElectiveCourse(int majorId, String courseId)
    {
        String sql1 = "select coursetypeinmajor from course_with_major cm where cm.majorid = ? and cm.courseid = ?";
        String sql2 = "insert into course_with_major (courseid, majorid, coursetypeinmajor) values (?,?,?)";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, majorId);
            stmt1.setString(2, courseId);
            ResultSet resultSet = stmt1.executeQuery();
            if(resultSet.next())
                throw new IntegrityViolationException();

            stmt2.setString(1, courseId);
            stmt2.setInt(2, majorId);
            stmt2.setInt(3, 1);
            //1代表选修课
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}
