package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.CourseService;
import org.postgresql.replication.PGReplicationConnection;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import javax.print.DocFlavor;
import javax.swing.plaf.IconUIResource;
import java.sql.*;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.SplittableRandom;

@ParametersAreNonnullByDefault
public class CourseServiceImplement implements CourseService
{

    @Override
    public void addCourse(String courseId, String courseName, int credit, int classHour,
                          Course.CourseGrading grading, @Nullable Prerequisite prerequisite)
    {
        String sql1 = "select coalesce(max(index), 0) as number from prerequisite ";

        String sql = "insert into course(courseid, coursehour, credit, name, grading)"
                + "values(?,?,?,?,?)";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            PreparedStatement stmt1 = connection.prepareStatement(sql1))
        {
            int number = 0;
            ResultSet resultSet = stmt1.executeQuery();
            resultSet.next();
            number = resultSet.getInt("number");


            stmt.setString(1, courseId);
            stmt.setInt(2, classHour);
            stmt.setInt(3, credit);
            stmt.setString(4, courseName);
            if(grading.toString().equals("HUNDRED_MARK_SCORE"))
                stmt.setInt(5, 100);
            else
                stmt.setInt(5,2);
            stmt.executeUpdate();

            if(prerequisite != null)
                constructPrere(prerequisite, number + 1, courseId);
        }
        catch (SQLException e)
        {
            throw new IntegrityViolationException();
        }
    }


    @Override
    public synchronized int addCourseSection(String courseId, int semesterId, String sectionName, int totalCapacity)
    {
        String sql = "insert into coursesection (courseid, semesterid, name, totalcapacity, leftcapacity) values (?,?,?,?,?) returning sectionid;";
        String sql1 = "select * from course where courseid = ?";
        String sql2 = "select max(cs.sectionid) as sectionid from coursesection cs";
        String sql3 = "select cs.sectionid as sectionid from coursesection cs where cs.courseid = ? and cs.semesterid = ? and cs.name = ? and cs.totalcapacity = ?";

        int number = 0;

        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt3 = connection.prepareStatement(sql3))
        {
            stmt3.setString(1, courseId);
            stmt3.setInt(2, semesterId);
            stmt3.setString(3, sectionName);
            stmt3.setInt(4, totalCapacity);

            ResultSet resultSet3 = stmt3.executeQuery();
            if(resultSet3.next())
                throw new IntegrityViolationException();

            stmt1.setString(1, courseId);

            ResultSet resultSet1 = stmt1.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt.setString(1, courseId);
            stmt.setInt(2, semesterId);
            stmt.setString(3, sectionName);
            stmt.setInt(4, totalCapacity);
            stmt.setInt(5, totalCapacity);

            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            number = resultSet.getInt("sectionid");

            ResultSet resultSet2 = stmt2.executeQuery();
            resultSet2.next();
            //number = resultSet1.getInt("sectionid");
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return number;
    }


    @Override
    public synchronized int addCourseSectionClass(int sectionId, int instructorId, DayOfWeek dayOfWeek, Set<Short> weekList,
                                     short classStart, short classEnd, String location)
    {
        String sql1 = "select * from coursesection where sectionid = ?;";
        String sql = "insert into coursesectionclass (sectionid, instructorId, dayOfweek, " +
                "weekList, classbegin, classEnd, location) " +
                "values (?,?,?,?,?,?,?);";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt1.setInt(1, sectionId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new IntegrityViolationException();

            Integer[] new_weeklist = new Integer[weekList.size()];

            int i = 0;
            for (short a : weekList)
            {
                new_weeklist[i] = (int)a;
                i++;
            }
//            for (int i = 0; i < weekList.size(); i++)
//            {
//                int a = weekList.get(i);
//                new_weeklist[i] = a;
//            }

            stmt.setInt(1, sectionId);
            stmt.setInt(2, instructorId);
            stmt.setString(3, dayOfWeek.toString());
            stmt.setArray(4, connection.createArrayOf("integer", new_weeklist));
            stmt.setInt(5, (int)classStart);
            stmt.setInt(6, (int)classEnd);
            stmt.setString(7, location);

            stmt.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public void removeCourse(String courseId)
    {
        String sql1 = "select * from course where courseid = ?";
        String sql2 = "delete from course where courseid = ?";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setString(1, courseId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setString(1, courseId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void removeCourseSection(int sectionId)
    {
        String sql1 = "select * from coursesectionclass where sectionid = ? ";
        String sql2 = "delete from coursesection where sectionid = ?";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);)
        {
            stmt1.setInt(1, sectionId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public void removeCourseSectionClass(int classId)
    {
        String sql1 = "select * from coursesectionclass where classid = ?";
        String sql2 = "delete from coursesectionclass where courseid = ?;";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, classId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, classId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<Course> getAllCourses()
    {
        List<Course> courses = new ArrayList<>();
        String sql = "select * from Course";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next())
            {
                Course course = new Course();
                course.id = resultSet.getString("courseid");
                course.classHour = resultSet.getInt("courseHour");
                course.credit = resultSet.getInt("credit");
                course.name = resultSet.getString("name");
                Course.CourseGrading courseGrading = Course.CourseGrading.PASS_OR_FAIL;
                if(resultSet.getString("grading").equals("HUNDRED_MARK_SCORE"))
                    courseGrading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                course.grading = courseGrading;
                courses.add(course);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return courses;
    }


    @Override
    public List<CourseSection> getCourseSectionsInSemester(String courseId, int semesterId)
    {
        List<CourseSection> courseSections = new ArrayList<>();
        String sql1 = "select * from course where courseid = ?";
        String sql2 = "select * from semester where id = ?";
        String sql3 = "select from coursesection where courseid = ? and semesterid = ?";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt3 = connection.prepareStatement(sql3))
        {
            stmt1.setString(1, courseId);
            ResultSet resultSet1 = stmt1.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, semesterId);
            resultSet1 = stmt2.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt3.setString(1, courseId);
            stmt3.setInt(2, semesterId);
            ResultSet resultSet = stmt3.executeQuery();

            while (resultSet.next())
            {
                CourseSection courseSection = new CourseSection();
                courseSection.id = resultSet.getInt("sectionid");
                courseSection.name = resultSet.getString("name");
                courseSection.totalCapacity = resultSet.getInt("totalcapacity");
                courseSection.leftCapacity = resultSet.getInt("leftcapacity");
                courseSections.add(courseSection);
            }
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return courseSections;
    }

    @Override
    public Course getCourseBySection(int sectionId)
    {
        String sql1 = "select * from coursesection where sectionid = " + sectionId + ";";
        String sql = "select * from course join courseSection cS on cS.sectionId = ? "
                + "where course.courseid = cS.courseid";
        Course course = new Course();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt1.executeQuery();
            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt.setInt(1, sectionId);
            resultSet = stmt.executeQuery();
            resultSet.next();
            course.id = resultSet.getString("courseid");
            course.classHour = resultSet.getInt("courseHour");
            course.credit = resultSet.getInt("credit");
            course.name = resultSet.getString("name");
            course.grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
            if(resultSet.getInt("grading") == 0)
                course.grading = Course.CourseGrading.PASS_OR_FAIL;

        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return course;
    }

    @Override
    public List<CourseSectionClass> getCourseSectionClasses(int sectionId)
    {
        String sql1 = "select * from coursesection where sectionid = " + sectionId + ";";
        String sql = "select * from coursesectionclass where sectionid = ?";
        List<CourseSectionClass> courseSectionClasses = new ArrayList<>();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt1.executeQuery();
            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt.setInt(1, sectionId);
            ResultSet resultSet1 = stmt.executeQuery();
            while (resultSet1.next())
            {
                CourseSectionClass courseSectionClass = new CourseSectionClass();
                courseSectionClass.id = resultSet1.getInt("classid");
            }
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return null;
    }

    @Override
    public CourseSection getCourseSectionByClass(int classId)
    {
        String sql1 = "select * from coursesectionclass where classid = " + classId + ";";
        String sql = "select * from coursesection join coursesectionclass cSC on cSC.classid = ? "
                + "where coursesection.sectionid = cSC.sectionid";
        CourseSection courseSection = new CourseSection();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt1.setInt(1, classId);
            ResultSet resultSet1 = stmt1.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt.setInt(1, classId);
            ResultSet resultSet = stmt.executeQuery();
            resultSet.next();
            courseSection.id = resultSet.getInt("sectionid");
            courseSection.name = resultSet.getString("name");
            courseSection.totalCapacity = resultSet.getInt("totalCapacity");
            courseSection.leftCapacity = resultSet.getInt("leftCapacity");

        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return courseSection;
    }

    @Override
    public List<Student> getEnrolledStudentsInSemester(String courseId, int semesterId)
    {
        String sql1 = "select * from course where courseid = ?;" ;
        String sql2 = "select * from semester where id = ?;";
        String sql = "select student.studentid, student.fullname, student.enrolleddate,"
                + "m.majorid, m.name as majorname, m.department, d.id, d.name as departmentname " +
                "from student " +
                "join student_with_section sws on student.studentid = sws.studentid " +
                "join coursesection cs on cs.courseid = ? and cs.semesterid = ? and sws.sectionid = cs.sectionid " +
                "join major m on student.major = m.majorid " +
                "join department d on m.department = m.department";
        List<Student> students = new ArrayList<>();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt1.setString(1, courseId);
            ResultSet resultSet1 = stmt1.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, semesterId);
            resultSet1 = stmt2.executeQuery();
            if(!resultSet1.next())
                throw new EntityNotFoundException();

            stmt.setString(1, courseId);
            stmt.setInt(2, semesterId);
            ResultSet resultSet = stmt.executeQuery();
            while (resultSet.next())
            {
                Student student = new Student();
                student.id = resultSet.getInt("studentid");
                student.fullName = resultSet.getString("fullname");
                student.enrolledDate = resultSet.getDate("emrolledDate");
                student.major = new Major();
                student.major.id = resultSet.getInt("majorid");
                student.major.name = resultSet.getString("majorname");
                student.major.department = new Department();
                student.major.department.id = resultSet.getInt("id");
                student.major.department.name = resultSet.getString("departmentname");
                students.add(student);
            }
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return students;
    }


    public int constructPrere(Prerequisite nowPrerequisite, int index, String main_course_id)
    {
        int index_can_use = index;

        //String sql = "";
        int size = 1;
        if(nowPrerequisite instanceof AndPrerequisite)
            size = ((AndPrerequisite) nowPrerequisite).terms.size();
        else if(nowPrerequisite instanceof OrPrerequisite)
            size = ((OrPrerequisite) nowPrerequisite).terms.size();
        else
            size = 1;

        List<Integer> childs = new ArrayList<>(1);
        String relation = "";

        if(nowPrerequisite instanceof AndPrerequisite)
        {
            //size = ((AndPrerequisite) nowPrerequisite).terms.size();
            //childs = new ArrayList<>(size);
            int now = 0;
            for (Prerequisite pre : ((AndPrerequisite) nowPrerequisite).terms)
            {
                childs.add(index_can_use + 1);
                index_can_use = constructPrere(pre, index_can_use + 1, main_course_id);
                //childs.set(now++, index_can_use);
            }
            relation = "and";
        }
        else if(nowPrerequisite instanceof OrPrerequisite)
        {
            //size = ((OrPrerequisite) nowPrerequisite).terms.size();
            //childs = new ArrayList<>(size);
            //childs = new int[size];
            int now = 0;
            for (Prerequisite pre : ((OrPrerequisite) nowPrerequisite).terms)
            {
                childs.add(index_can_use + 1);
                index_can_use = constructPrere(pre, index_can_use + 1, main_course_id);
                //childs.set(now++, index_can_use);
                //childs[now++] = index_can_use;
            }
            relation = "or";
        }
        else if(nowPrerequisite instanceof CoursePrerequisite)
        {
            //childs = new ArrayList<>(1);
            //childs.set(0, -1);
            childs.add(-1);
            relation = ((CoursePrerequisite) nowPrerequisite).courseID;
        }

        Integer[] intgers = new Integer[size];
        for (int i = 0; i < size; i++)
        {
            intgers[i] = childs.get(i);
        }
        String sql = "insert into prerequisite(courseid, index, relation, child) values (?,?,?,?);";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            //connection.createArrayOf()
            stmt.setString(1, main_course_id);
            stmt.setInt(2, index);
            stmt.setString(3, relation);
            stmt.setArray(4, connection.createArrayOf("integer", intgers));
            stmt.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }

        return index_can_use;
    }
}
