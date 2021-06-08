package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Semester;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.SemesterService;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

public class SemesterServiceImplement implements SemesterService
{
    @Override
    public int addSemester(String name, Date begin, Date end)
    {
        String sql1 = "select s.id from semester s where s.name = ? and s.beginTime = ? and s.endTime = ?;";
        String sql2 = "insert into semester(name, begintime, endtime)"
                + "values(?,?,?)";
        int number = 0;
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt2.setString(1, name);
            stmt2.setDate(2, begin);
            stmt2.setDate(3, end);
        }
        catch (SQLException e)
        {
            throw new IntegrityViolationException();
        }
        return number;
    }

    @Override
    public void removeSemester(int semesterId)
    {
        String sql1 = "select s.name from semester s where s.id = ?";
        String sql2 = "delete from semester where id = ?";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, semesterId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, semesterId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<Semester> getAllSemesters()
    {
        List<Semester> semesters = new ArrayList<>();
        String sql = "select * from semester";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next())
            {
                Semester semester = new Semester();
                semester.id = resultSet.getInt("id");
                semester.name = resultSet.getString("name");
                semester.begin = resultSet.getDate("begintime");
                semester.end = resultSet.getDate("endtime");
                semesters.add(semester);
            }
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return semesters;
    }

    @Override
    public Semester getSemester(int semesterId)
    {
        String sql = "select * from semester where id = ?";
        Semester semester = new Semester();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt.setInt(1, semesterId);
            ResultSet resultSet = stmt.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();
            semester.id = resultSet.getInt("id");
            semester.name = resultSet.getString("name");
            semester.begin = resultSet.getDate("begintime");
            semester.end = resultSet.getDate("endtime");
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return semester;
    }
}
