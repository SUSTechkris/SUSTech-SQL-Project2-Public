package reference;

import cn.edu.sustech.cs307.factory.ServiceFactory;
import cn.edu.sustech.cs307.service.*;

public class FactoryImplement extends ServiceFactory
{
    public FactoryImplement()
    {
        registerService(CourseService.class, new CourseServiceImplement());
        registerService(DepartmentService.class, new DepartmentServiceImplement());
        registerService(InstructorService.class, new InstructorServiceImplement());
        registerService(MajorService.class, new MajorServiceImplement());
        registerService(SemesterService.class, new SemesterServiceImplement());
        registerService(StudentService.class, new StudentServiceImplement());
        registerService(UserService.class, new UserServiceImplement());
    }
}
