package reference;


import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Course;
import cn.edu.sustech.cs307.dto.CourseSearchEntry;
import cn.edu.sustech.cs307.dto.CourseTable;
import cn.edu.sustech.cs307.dto.Major;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.service.StudentService;
import io.netty.util.internal.PriorityQueue;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

@ParametersAreNonnullByDefault
public class StudentServiceImplement implements StudentService
{
    @Override
    public void addStudent(int userId, int majorId, String firstName, String lastName, Date enrolledDate)
    {
        String sql = "insert into users(userid, character) values (?,?)";
        String sql1 = "insert into student(studentId, major, firstname, lastname, enrolledDate)"
                + "values(?,?,?,?,?)";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            PreparedStatement stmt1 = connection.prepareStatement(sql1))
        {
            stmt.setInt(1, userId);
            stmt.setInt(2, 2);
            stmt.executeUpdate();


            stmt1.setInt(1, userId);
            stmt1.setInt(2, majorId);
            stmt1.setString(3, firstName);
            stmt1.setString(4, lastName);
            stmt1.setDate(5, enrolledDate);

            stmt1.executeUpdate();
            //return EnrollResult.SUCCESS;
        }catch (SQLException e)
        {
            e.printStackTrace();
            //return EnrollResult.COURSE_CONFLICT_FOUND;
        }

    }

    @Override
    public List<CourseSearchEntry> searchCourse(int studentId, int semesterId, @Nullable String searchCid, @Nullable String searchName,
                                                @Nullable String searchInstructor, @Nullable DayOfWeek searchDayOfWeek,
                                                @Nullable Short searchClassTime, @Nullable List<String> searchClassLocations,
                                                CourseType searchCourseType, boolean ignoreFull, boolean ignoreConflict,
                                                boolean ignorePassed, boolean ignoreMissingPrerequisites, int pageSize, int pageIndex)
    {
        return null;
    }

    @Override
    public EnrollResult enrollCourse(int studentId, int courseId)
    {
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement("insert into student_course(student_id, course_id) values (?,?);"))
        {
            stmt.setInt(1, studentId);
            stmt.setInt(2, courseId);
            stmt.executeUpdate();
            return EnrollResult.SUCCESS;
        }catch (SQLException e)
        {
            e.printStackTrace();
            return EnrollResult.COURSE_CONFLICT_FOUND;
        }
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException
    {
        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt = connection.prepareStatement("call drop_course(?,?)"))
        {
            stmt.setInt(1, studentId);
            stmt.setInt(2, sectionId);
            stmt.execute();
        }catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade)
    {
//        String sql = " ";
//        if(grade == null)
//        {
//            sql = "insert into "
//        }
//        else if(grade instanceof HundredMarkGrade)
//        {
//
//        }
//        //grade.when(new HundredMarkGrade())
//        sql = "insert into student(studentId, , name, enrolledDate)"
//                + "values(?,?,?,?)";
//        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
//            PreparedStatement stmt = connection.prepareStatement(sql))
//        {
//            stmt.setInt(1, userId);
//            stmt.setInt(2, majorId);
//            stmt.setString(3, full_name);
//            stmt.setString(4, enrolledDate.toString());
//            stmt.executeUpdate();
//            //return EnrollResult.SUCCESS;
//        }catch (SQLException e)
//        {
//            e.printStackTrace();
//            //return EnrollResult.COURSE_CONFLICT_FOUND;
//        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade)
    {

    }

    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId)
    {
//        String sql1 = "select * from coursesectionclass where classid = ?";
//        String sql2 = "delete from coursesectionclass where courseid = ?;";
//        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
//            PreparedStatement stmt1 = connection.prepareStatement(sql1);
//            PreparedStatement stmt2 = connection.prepareStatement(sql2))
//        {
//            stmt1.setInt(1, classId);
//            ResultSet resultSet = stmt1.executeQuery();
//
//            if(!resultSet.next())
//                throw new EntityNotFoundException();
//
//            stmt2.setInt(1, classId);
//            stmt2.executeUpdate();
//        }
//        catch (SQLException e)
//        {
//            throw new EntityNotFoundException();
//        }
        return null;
    }

    @Override
    public CourseTable getCourseTable(int studentId, Date date)
    {
        return null;
    }

    @Override
    public boolean passedPrerequisitesForCourse(int studentId, String courseId)
    {
        String sql1 = "select * from prerequisite where courseid = ?;";
        String sql2 = "select c.courseid as courseid from student_with_section sws \n" +
                "    join coursesection cs on sws.sectionid = cs.sectionid\n" +
                "    join course c on cs.courseid = c.courseid\n" +
                "where studentid = ?";
        //String sql1 = "select * from prerequisite where index = 1";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setString(1, courseId);
            ResultSet resultSet = stmt1.executeQuery();
            int count = 0;

            java.util.PriorityQueue<Node> heap = new java.util.PriorityQueue<>(new Comparator<Node>()
            {
                @Override
                public int compare(Node o1, Node o2)
                {
                    return o2.index - o1.index;
                }
            });
            while (resultSet.next())
            {
                count++;
                Node node = new Node(resultSet.getString("courseid"), resultSet.getInt("index")
                        ,resultSet.getString("relation"), sql_array_to_array(resultSet.getArray("child")));
                heap.add(node);
            }
            if(count == 0)
                return true;

            ArrayList<String> have_course = new ArrayList<>();
//            have_course.add("c");
//            have_course.add("b");
//            have_course.add("f");
//            have_course.add("g");
            ArrayList<Integer> satisfy = new ArrayList<>();
            ArrayList<Integer> not_satisfy = new ArrayList<>();
            stmt2.setInt(1, studentId);
            ResultSet resultSet1 = stmt2.executeQuery();
            while (resultSet1.next())
            {
                have_course.add(resultSet1.getString("courseid"));
            }

            int smallest = 0;
            while (!heap.isEmpty())
            {
                Node node = heap.poll();
                if(heap.isEmpty())
                {
                    smallest = node.index;
                }
                if(node.relation.equals("and"))
                {
                    int count1 = 0;
                    for (int number : node.child)
                    {
                        if(satisfy.contains(number))
                            count1++;
                    }
                    if(count1 == node.child.length)
                        satisfy.add(node.index);
                    else
                        not_satisfy.add(node.index);
                }
                else if(node.relation.equals("or"))
                {
                    boolean ifornot = false;
                    for (int number : node.child)
                    {
                        if(satisfy.contains(number))
                        {
                            ifornot = true;
                            break;
                        }
                    }
                    if(ifornot)
                        satisfy.add(node.index);
                    else
                        not_satisfy.add(node.index);
                }
                else
                {
                    if(have_course.contains(node.relation))
                        satisfy.add(node.index);
                    else
                        not_satisfy.add(node.index);
                }
            }
            if(satisfy.contains(smallest))
                return true;
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return false;
    }

    @Override
    public Major getStudentMajor(int studentId)
    {
        return null;
    }

    public static int[] sql_array_to_array(Array array)
    {
        String string = array.toString().substring(1, array.toString().length() - 1);
        String[] strings = string.split(",");
        int[] numbers = new int[strings.length];
        for (int i = 0; i < numbers.length; i++)
        {
            numbers[i] = Integer.valueOf(strings[i]);
        }
        return numbers;
    }

    static class Node
    {
        public String courseid;
        public int index;
        public String relation;
        public int[] child;

        public Node(String courseid, int node, String relation, int[] child)
        {
            this.courseid = courseid;
            this.index = node;
            this.relation = relation;
            this.child = child;
        }
    }
}
