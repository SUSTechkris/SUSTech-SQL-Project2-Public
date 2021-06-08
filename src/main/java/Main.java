import cn.edu.sustech.cs307.config.Config;
import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.dto.User;
import cn.edu.sustech.cs307.dto.prerequisite.AndPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.CoursePrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.OrPrerequisite;
import cn.edu.sustech.cs307.dto.prerequisite.Prerequisite;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import reference.*;

import java.sql.*;
import java.time.DayOfWeek;
import java.util.ArrayList;
import java.util.List;

import static reference.StudentServiceImplement.sql_array_to_array;

public class Main
{
    public static void main(String[] args)
    {
        FactoryImplement factoryImplement = new FactoryImplement();

        DepartmentServiceImplement departSI = new DepartmentServiceImplement();
        InstructorServiceImplement instrSI = new InstructorServiceImplement();
        MajorServiceImplement majorSI = new MajorServiceImplement();
        UserServiceImplement userSI = new UserServiceImplement();
        CourseServiceImplement courSI = new CourseServiceImplement();
        StudentServiceImplement studSI = new StudentServiceImplement();
        //departSI.addDepartment("aaa");
        //departSI.addDepartment("bbb");
        //departSI.removeDepartment(123);
        //Department department = departSI.getDepartment(123);
        //System.out.println(department.id);
        //System.out.println(department.name);
        //instrSI.addInstructor(1,"a", "aa");
        //majorSI.addMajor("aaa", 1);
        //departSI.getDepartment(3);
        //majorSI.getMajor(3);
        //departSI.addDepartment("aaa");
        //instrSI.addInstructor(3, "吴", "一凡" );
        //User user = userSI.getUser(3);
        //System.out.println(user.fullName);
        CoursePrerequisite b = new CoursePrerequisite("b");
        CoursePrerequisite c = new CoursePrerequisite("c");
        CoursePrerequisite d = new CoursePrerequisite("d");
        CoursePrerequisite e = new CoursePrerequisite("e");
        CoursePrerequisite f = new CoursePrerequisite("f");
        CoursePrerequisite g = new CoursePrerequisite("g");

        List<Prerequisite> and2_list = new ArrayList<>();
        and2_list.add(f);
        and2_list.add(g);
        AndPrerequisite and2 = new AndPrerequisite(and2_list);

        List<Prerequisite> or2_list = new ArrayList<>();
        or2_list.add(e);
        or2_list.add(and2);
        OrPrerequisite or2 = new OrPrerequisite(or2_list);

        List<Prerequisite> or1_list = new ArrayList<>();
        or1_list.add(c);
        or1_list.add(d);
        OrPrerequisite or1 = new OrPrerequisite(or1_list);

        List<Prerequisite> and1_list = new ArrayList<>();
        and1_list.add(b);
        and1_list.add(or1);
        and1_list.add(or2);
        AndPrerequisite and1 = new AndPrerequisite(and1_list);

        courSI.constructPrere(and1, 1, "a");

//        String sql1 = "select * from prerequisite where index = 1";
//        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
//            PreparedStatement stmt1 = connection.prepareStatement(sql1))
//        {
//            ResultSet resultSet = stmt1.executeQuery();
//            if(!resultSet.next())
//                throw new EntityNotFoundException();
//
//            Array array = resultSet.getArray("child");
//            int[] numbers = sql_array_to_array(array);
//            for (int i = 0; i < numbers.length; i++)
//            {
//                System.out.println(numbers[i]);
//            }
//
//            //ArrayList<Integer> arrayList = array.asList();
//            //Integer integers = (Integer) array.getArray();
//            //System.out.println(array.getArray());
//
//        }
//        catch (SQLException e)
//        {
//            throw new EntityNotFoundException();
//        }
        studSI.passedPrerequisitesForCourse(1, "a");
    }
}
