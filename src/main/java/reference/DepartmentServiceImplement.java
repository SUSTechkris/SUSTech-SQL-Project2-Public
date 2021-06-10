package reference;

import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.Department;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.DepartmentService;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;

public class DepartmentServiceImplement implements DepartmentService
{

    @Override
    public int addDepartment(String name)
    {
        String sql1 = "select d.id as number from department d where d.name = ?;";
        String sql2 = "insert into department(name) values (?)";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            //stmt1.setString(1, name);
            //ResultSet resultSet = stmt1.executeQuery();
            stmt2.setString(1, name);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new IntegrityViolationException();
        }
        return 0;
    }

    //wyfwyfwyf
    @Override
    public void removeDepartment(int departmentId)
    {
        String sql1 = "select d.name from department d where id = ?";
        String sql2 = "delete from department where department.id = ?";
        int number = 0;
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt1.setInt(1, departmentId);
            ResultSet resultSet = stmt1.executeQuery();

            if(!resultSet.next())
                throw new EntityNotFoundException();

            stmt2.setInt(1, departmentId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
    }

    @Override
    public List<Department> getAllDepartments()
    {
        String sql = "select d.id as departmentid, d.name as departmentname from department d;";
        List<Department> departments = new ArrayList<>();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            ResultSet resultSet = stmt.executeQuery();
            int count = 0;
            while (resultSet.next())
            {
                count++;
                Department department = new Department();
                department.id = resultSet.getInt("departmentid");
                department.name = resultSet.getString("departmentname");
                departments.add(department);
            }
//            if(count == 0)
//                throw new EntityNotFoundException();
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return departments;
    }

    @Override
    public Department getDepartment(int departmentId)
    {
        String sql = "select d.id as departmentid, d.name as departmentname\n" +
                "from department d where id = ?;";
        Department department = new Department();
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql))
        {
            stmt.setInt(1, departmentId);
            ResultSet resultSet = stmt.executeQuery();
            department.id = resultSet.getInt("departmentid");
            department.name = resultSet.getString("departmentname");
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return department;
    }
}
