package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Course;
import cn.edu.sustech.cs307.dto.CourseSection;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.CourseService;
import cn.edu.sustech.cs307.service.InstructorService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class InstructorServiceImplement implements InstructorService
{
    @Override
    public void addInstructor(int userId, String firstName, String lastName)
    {
        String sql1 = "insert into users(userid, character) values (?, 1)";
        String sql2 = "insert into instructor(instructorid, firstname, lastname) values (?,?,?);";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, userId);
            stmt1.executeUpdate();

            stmt2.setInt(1, userId);
            stmt2.setString(2, firstName);
            stmt2.setString(3, lastName);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new IntegrityViolationException();
        }
    }

    @Override
    public List<CourseSection> getInstructedCourseSections(int instructorId, int semesterId)
    {
        List<CourseSection> courseSections = new ArrayList<>();
        String sql1 = "select cs.sectionId as sectionid, cs.name as name, cs.totalCapacity as totalCapacity,\n" +
                "cs.leftCapacity as leftCapacity from courseSection cs\n" +
                "join courseSectionClass csc on cs.sectionId = csc.sectionId\n" +
                "join semester s on cs.semesterId = s.id\n" +
                "where csc.instructorId = ? and s.id = ?;";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1))
        {
            stmt1.setInt(1, instructorId);
            stmt1.setInt(2, semesterId);
            ResultSet resultSet = stmt1.executeQuery();
            int count = 0;
            while (resultSet.next())
            {
                count++;
                CourseSection courseSection = new CourseSection();
                courseSection.id = resultSet.getInt("sectionid");
                courseSection.name = resultSet.getString("name");
                courseSection.totalCapacity = resultSet.getInt("totalCapacity");
                courseSection.leftCapacity = resultSet.getInt("leftCapacity");
                courseSections.add(courseSection);
            }
            if(count == 0)
                throw new EntityNotFoundException();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return courseSections;
    }
}
