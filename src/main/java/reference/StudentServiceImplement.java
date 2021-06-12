package reference;


import cn.edu.sustech.cs307.database.SQLDataSource;
import cn.edu.sustech.cs307.dto.*;
import cn.edu.sustech.cs307.dto.grade.Grade;
import cn.edu.sustech.cs307.dto.grade.HundredMarkGrade;
import cn.edu.sustech.cs307.dto.grade.PassOrFailGrade;
import cn.edu.sustech.cs307.exception.EntityNotFoundException;
import cn.edu.sustech.cs307.exception.IntegrityViolationException;
import cn.edu.sustech.cs307.service.StudentService;
import io.netty.util.internal.PriorityQueue;

import javax.annotation.Nullable;
import javax.annotation.ParametersAreNonnullByDefault;
import java.sql.*;
import java.sql.Date;
import java.time.DayOfWeek;
import java.util.*;

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
    public
    List<CourseSearchEntry>
    searchCourse(int studentId, int semesterId, @Nullable String searchCid,
                 @Nullable String searchName, @Nullable String searchInstructor,
                 @Nullable DayOfWeek searchDayOfWeek, @Nullable Short searchClassTime,
                 @Nullable List<String> searchClassLocations,
                 CourseType searchCourseType,
                 boolean ignoreFull, boolean ignoreConflict,
                 boolean ignorePassed, boolean ignoreMissingPrerequisites,
                 int pageSize, int pageIndex)
    {
        List<CourseSearchEntry> courseSearchEntries = new ArrayList<>();
        int[] arr = new int[12];
        for(int i = 0; i < 12; i++)
        {
            arr[i] = 0;
        }
        int pos = 1;
        String s1 = " ";
        String s2 = " ";
        String s3 = " ";
        String s4 = " ";
        String s5 = " ";
        String s6 = " ";
        String s7 = " ";
        String s8 = " ";
        String s9 = " ";
        String s10 = " ";
        String s11 = " ";
        String sql="select * from course c1 " +
                "join Coursesection c2 " +
                "on c1.courseid = c2.courseid " +
                "join coursesectionclass c3 " +
                "on c2.sectionid = c3.sectionid " +
                "join prerequisite p " +
                "on p.courseid = c1.courseid " +
                "where semesterid = ? ";
        if(searchCourseType==CourseType.ALL)
            s1="  ";
        else if(searchCourseType==CourseType.MAJOR_COMPULSORY){
            s1=" and c1.courseid in(select cwm.courseid from course_with_major cwm where cwm.majorid in (select stu.major from student stu where stu.studentid=?) and cwm.coursetypeinmajor=0) ";
            arr[1]=pos+1;
            pos++;
        }
        else if(searchCourseType==CourseType.MAJOR_ELECTIVE){
            s1=" and c1.courseid in (select cwm.courseid from course_with_major cwm " +
                    "where cwm.majorid not in (select stu.major from student stu where stu.studentid=?) and cwm.coursetypeinmajor=1) ";
            arr[1]=pos+1;
            pos++;
        }
        else if(searchCourseType==CourseType.CROSS_MAJOR){
            s1=" and c1.courseid in (select cwm.courseid from course_with_major cwm " +
                    "where cwm.majorid not in (select stu.major from student stu where stu.studentid=?)) ";
            arr[1]=pos+1;
            pos++;
        }
        else if(searchCourseType==CourseType.PUBLIC){
            s1=" and c1.courseid not in (select cwm.courseid from course_with_major) ";
        }
        if(!ignoreFull)
            s2=" and c2.leftcapacity <> 0 ";
        ArrayList<Integer> COURSEID=new ArrayList<>();
        if(!ignoreConflict)
        {//   !!!!!!!!!!!!!!!!!!!记得补上参数   ！！！  arr  pos   ...
            String sqlt = "select ccs.dayOfWeek,ccs.weekList,ccs.classBegin,ccs.classBegin,c.name\n" +
                    "from  student_with_section sc\n" +
                    "join courseSection cs\n" +
                    "on cs.sectionid=sc.sectionid\n" +
                    "join course c\n" +
                    "on c.courseid=cs.courseId\n" +
                    "join courseSectionClass ccs\n" +
                    "on ccs.sectionid=sc.sectionid\n" +
                    "where sc.studentid=?;";


            ArrayList<CourseSectionClass> class_have = new ArrayList<>();
            try(Connection conn_t = SQLDataSource.getInstance().getSQLConnection();
                PreparedStatement ps_t = conn_t.prepareStatement(sqlt))
            {
                ps_t.setInt(1, studentId);
                ResultSet rsst = ps_t.executeQuery(sqlt);
                //int grade=rss_t.getInt("s.grade");
                while(rsst.next()){
                    CourseSectionClass courseSectionClass = new CourseSectionClass();
                    String day = rsst.getString("dayOfWeek");
                    switch (day) {
                        case "MONDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.MONDAY;
                            break;
                        case "TUESDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.TUESDAY;
                            break;
                        case "WEDNESDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.WEDNESDAY;
                            break;
                        case "THURSDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.THURSDAY;
                            break;
                        case "FRIDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.FRIDAY;
                            break;
                        case "SATURDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.SATURDAY;
                            break;
                        case "SUNDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.SUNDAY;
                            break;
                    }
                    courseSectionClass.classBegin = rsst.getShort("classBegin");
                    courseSectionClass.classEnd = rsst.getShort("classEnd");
                    courseSectionClass.weekList = sql_array_to_set(rsst.getArray("weeklist"));
                    class_have.add(courseSectionClass);

                }
            }
            catch (SQLException et)
            {
                et.printStackTrace();
            }
            String sqltt="select ccs.dayOfWeek,ccs.weekList,ccs.classBegin,ccs.classBegin,c.name \n" +
                    "from courseSectionClass ccs\n" +
                    "join courseSection cs\n" +
                    "on cs.sectionid=ccs.sectionid\n" +
                    "join course c\n" +
                    "on c.courseid=cs.courseId";

            ArrayList<CourseSectionClass> class_want = new ArrayList<>();
            try(Connection conn_tt = SQLDataSource.getInstance().getSQLConnection();
                PreparedStatement ps_tt = conn_tt.prepareStatement(sqltt))
            {
                ResultSet rsstt = ps_tt.executeQuery(sqltt);
                while(rsstt.next()){
                    CourseSectionClass courseSectionClass = new CourseSectionClass();
                    String day = rsstt.getString("dayOfWeek");
                    switch (day) {
                        case "MONDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.MONDAY;
                            break;
                        case "TUESDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.TUESDAY;
                            break;
                        case "WEDNESDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.WEDNESDAY;
                            break;
                        case "THURSDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.THURSDAY;
                            break;
                        case "FRIDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.FRIDAY;
                            break;
                        case "SATURDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.SATURDAY;
                            break;
                        case "SUNDAY":
                            courseSectionClass.dayOfWeek = DayOfWeek.SUNDAY;
                            break;
                    }
                    courseSectionClass.classBegin = rsstt.getShort("classBegin");
                    courseSectionClass.classEnd = rsstt.getShort("classEnd");
                    courseSectionClass.weekList = sql_array_to_set(rsstt.getArray("weeklist"));
                    class_want.add(courseSectionClass);
                }
            }
            catch (SQLException ett)
            {
                ett.printStackTrace();
            }
            for (CourseSectionClass this_class : class_want)
                for (CourseSectionClass old_class : class_have)
                    if(this_class.dayOfWeek.equals(old_class.dayOfWeek))
                        if(!(this_class.classEnd < old_class.classBegin || old_class.classEnd < this_class.classBegin))
                            for (Short now : this_class.weekList)
                                if(old_class.weekList.contains(now))
                                    COURSEID.add(this_class.id);
            s3="and c1.name not in (select c.name from  student_with_section sc\n" +
                    "join courseSection cs\n" +
                    "on cs.sectionid=sc.sectionid\n" +
                    "join course c\n" +
                    "on c.courseid=cs.courseId\n" +
                    "where sc.studentid=?) and ccs.classId not in (?) ";
            arr[3]=pos+1;
            arr[4]=pos+2;
            pos=pos+2;
        }

        if(!ignorePassed){
            s4=" and p.courseid not in (select p.courseId from pf_grade p where p.studentid=? and p.grade='PASS'\n" +
                    "union\n" +
                    "select m.courseId from mark_grade m where m.studentid=? and m.grade>59) ";
            arr[5]=pos+1;
            arr[6]=pos+2;
            pos=pos+2;
        }

        if(!ignoreMissingPrerequisites){
            s5=" and  p.courseid is not null ";
        }

        if(searchCid!=null){
            s6=" and c1.courseid=searchCid ";
        }
        if(searchName!=null){
            s7=" and c1.name=searchName ";
        }
        if(searchInstructor!=null){
            s8=" and c3.instructorid in (select ins.instructorid from instructor ins where ins.fullname=searchInstructor) ";
        }
        if(searchDayOfWeek==DayOfWeek.SUNDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'SUNDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.MONDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'MONDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.TUESDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'TUESDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.WEDNESDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'WEDNESDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.THURSDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'THURSDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.FRIDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'FRIDAY\') ";
        else if(searchDayOfWeek==DayOfWeek.SATURDAY)
            s9=" and c3.courseid in(select cour.courseid from coursesectionclass cour where cour.dayofweek=\'SATURDAY\') ";
        if(searchClassTime!=null){
            s10=" and c3.courseid in(select cour.courseid from coursesectionclass cour where searchClassTime between cour.classbegin and cour.classend ";
        }
        sql=sql+s1+s2+s3+s4+s5+s6+s7+s8+s9+s10+") and ( ";
        if(searchClassLocations!=null){
            //String st=" ";
            int cnt=0;
            int length=searchClassLocations.size();
            for(String location : searchClassLocations){
                if(cnt==(length-1)){
                    sql=sql+" c3.location="+location+" ) ";
                }
                else{
                    sql=sql+" c3.location="+location+" or ";
                }
                cnt++;
            }

        }
        sql=sql+" order by c1.courseid,c1.name limit ? offset ? ;";
        try(Connection conn = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement ps = conn.prepareStatement(sql))
        {
            ps.setInt(1,semesterId);
            if(arr[1]!=0){
                ps.setInt(arr[1],studentId);
            }
            if(arr[3]!=0)
            {
                ps.setInt(arr[3],studentId);
                Integer[] new_course = new Integer[COURSEID.size()];
                for (int i = 0; i < new_course.length; i++)
                {
                    new_course[i] = COURSEID.get(i);
                }
                ps.setArray(arr[4],conn.createArrayOf("integer", new_course));
            }
            ps.setInt(pos+1,pageIndex);
            ps.setInt(pos+2,pageIndex*pageSize);
            ResultSet rss = ps.executeQuery();
            while (rss.next())
            {
                CourseSearchEntry courseSearchEntry=new CourseSearchEntry();
                courseSearchEntry.course.id=rss.getString("courseid");
                courseSearchEntry.course.classHour = rss.getInt("courseHour");
                courseSearchEntry.course.credit = rss.getInt("credit");
                courseSearchEntry.course.name = rss.getString("name");
                Course.CourseGrading courseGrading = Course.CourseGrading.PASS_OR_FAIL;
                if(rss.getString("grading").equals("HUNDRED_MARK_SCORE"))
                    courseGrading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                courseSearchEntry.course.grading = courseGrading;

                courseSearchEntry.section.id = rss.getInt("sectionid");
                courseSearchEntry.section.name = rss.getString("name");
                courseSearchEntry.section.totalCapacity = rss.getInt("totalcapacity");
                courseSearchEntry.section.leftCapacity = rss.getInt("leftcapacity");
                String sql2="select * from courseSectionClass cse \n" +
                        "join instructor i on cse.instructorId = i.instructorId \n" +
                        "join users u on u.userId=i.instructorId \n" +
                        "where cse.sectionid=?;";
                Set<CourseSectionClass> sectionClasses=new HashSet<>();
                try(Connection conn2 = SQLDataSource.getInstance().getSQLConnection();
                    PreparedStatement ps2 = conn.prepareStatement(sql2)){
                    ps2.setInt(1,courseSearchEntry.section.id);
                    ResultSet rss2 = ps2.executeQuery();
                    while (rss2.next()){
                        CourseSectionClass courseSectionClass=new CourseSectionClass();
                        courseSectionClass.id=rss2.getInt("calssid");
                        courseSectionClass.instructor.id=rss2.getInt("userid");
                        courseSectionClass.instructor.fullName=rss2.getString("fullname");
                        //courseSectionClass.dayOfWeek=rss2.getString("dayofweek");
                        //courseSectionClass.weekList=rss2.getString("weeklist");
                        courseSectionClass.classBegin=rss2.getShort("classbegin");
                        courseSectionClass.classEnd=rss2.getShort("classend");
                        courseSectionClass.location=rss2.getString("location");
                        sectionClasses.add(courseSectionClass);
                    }
                }
                catch (SQLException e2)
                {
                    e2.printStackTrace();
                }
                String sql3="select * from course ";
                List<String> ConflictCourseNames=new ArrayList<>();
                try(Connection conn3 = SQLDataSource.getInstance().getSQLConnection();
                    PreparedStatement ps3 = conn.prepareStatement(sql3)){
                    ResultSet rss3 = ps3.executeQuery();
                    while (rss3.next()){
                        String name;
                        name=rss3.getString("ConflictCourseNames");

                        ConflictCourseNames.add(name);
                    }
                }
                catch (SQLException e3)
                {
                    e3.printStackTrace();
                }
                courseSearchEntry.sectionClasses=sectionClasses;
                courseSearchEntry.conflictCourseNames=ConflictCourseNames;
                courseSearchEntries.add(courseSearchEntry);
            }
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
        return courseSearchEntries;
    }


    /**
     * It is the course selection function according to the studentId and courseId.
     * The test case can be invalid data or conflict info, so that it can return 8 different
     * types of enroll results.
     *
     * It is possible for a student-course have ALREADY_SELECTED and ALREADY_PASSED or PREREQUISITES_NOT_FULFILLED.
     * Please make sure the return priority is the same as above in similar cases.
     * {@link cn.edu.sustech.cs307.service.StudentService.EnrollResult}
     *
     * To check whether prerequisite courses are available for current one, only check the
     * grade of prerequisite courses are >= 60 or PASS
     *
     * @param studentId
     * @param sectionId the id of CourseSection
     * @return See {@link cn.edu.sustech.cs307.service.StudentService.EnrollResult}
     */
    @Override//暂时没有判断 COURSE_CONFLICT_FOUND
    public EnrollResult enrollCourse(int studentId, int sectionId)
    {
        //EnrollResult.COURSE_NOT_FOUND
        String str0 = "select * from coursesection where sectionid = ?";

        //EnrollResult.ALREADY_ENROLLED
        String str1 = "select * from student_with_section sws where sws.studentid = ? and sws.studentid = ?";

        //EnrollResult.ALREADY_PASSED
        String str2 = "select sws.grade as grade\n" +
                "from student_with_section sws\n" +
                "where sws.sectionid in (select sectionId\n" +
                "                        from courseSection cs\n" +
                "                        where cs.courseid = (select cs.courseId from courseSection cs where cs.sectionid = ?))\n" +
                "  and sws.grade is not null";

        //EnrollResult.PREREQUISITES_NOT_FULFILLED
        String str3 = "select distinct cs.courseid as courseid from coursesection cs where sectionid = ?;";


        //EnrollResult.COURSE_CONFLICT_FOUND
        //选课冲突
        String str4 = "select cs.courseId as courseid\n" +
                "from student_with_section sws\n" +
                "         join courseSection cs on cs.sectionId = sws.sectionid\n" +
                "where cs.semesterId = (select cs1.semesterId from courseSection cs1 where cs1.sectionId = ?)\n" +
                "and sws.studentid = ?";
//        String str4 = "select c.courseid as courseid " +
//                "from student_with_section sws " +
//                "join courseSection cs on cs.sectionId = sws.sectionid " +
//                "join course c on c.courseid = cs.courseId " +
//                "where sws.studentid = ?";


        //时间冲突
        String str5 = "select classId, sectionId, instructorId, dayOfWeek, weekList, " +
                "classBegin, classEnd, location\n" +
                "from courseSectionClass where sectionId = ?";

        String str6 = "select csc.weekList, csc.dayOfWeek, csc.classBegin, csc.classEnd, csc.location\n" +
                "from courseSectionClass csc\n" +
                "         join courseSection cs on cs.sectionId = csc.sectionid\n" +
                "         join student_with_section sws on cs.sectionId = sws.sectionid\n" +
                "where sws.studentid = ?\n" +
                "  and cs.semesterId = (select semesterId from courseSection where sectionId = ?)";



        String sql0 = "select * from (select * from student_with_section s0 where s0.studentid=?) s\n" +
                "join courseSection cs on s.sectionid = cS.sectionId\n" +
                "where cs.courseId in (select c2.courseId from coursesection c2\n" +
                "where c2.sectionid=?);";

        //EnrollResult.COURSE_IS_FULL
        String str7 = "select leftCapacity from courseSection where sectionid = ?";

        //EnrollResult result = EnrollResult.SUCCESS;
        String str_insert = "insert into student_with_section(studentid, sectionid, grade) values (?,?,?)";


        try(Connection conn0 = SQLDataSource.getInstance().getSQLConnection();
            //PreparedStatement stmt = conn0.prepareStatement(str);
            PreparedStatement stmt0 = conn0.prepareStatement(str0);
            PreparedStatement stmt1 = conn0.prepareStatement(str1);
            PreparedStatement stmt2 = conn0.prepareStatement(str2);
            PreparedStatement stmt3 = conn0.prepareStatement(str3);
            PreparedStatement stmt4 = conn0.prepareStatement(str4);
            PreparedStatement stmt5 = conn0.prepareStatement(str5);
            PreparedStatement stmt6 = conn0.prepareStatement(str6);
            PreparedStatement stmt7 = conn0.prepareStatement(str7);
//            PreparedStatement stmt8 = conn0.prepareStatement(str8);
//            PreparedStatement stmt9 = conn0.prepareStatement(str9);

            PreparedStatement insert = conn0.prepareStatement(str_insert);

            PreparedStatement ps0 = conn0.prepareStatement(sql0))
        {

            //EnrollResult.COURSE_NOT_FOUND
            stmt0.setInt(1, sectionId);
            ResultSet resultSet0 = stmt0.executeQuery();
            if(!resultSet0.next())
                return EnrollResult.COURSE_NOT_FOUND;
            String courseid = resultSet0.getString("courseid");

            //EnrollResult.ALREADY_ENROLLED
            stmt1.setInt(1, studentId);
            stmt1.setInt(2, sectionId);
            ResultSet resultSet1 = stmt1.executeQuery();
            if(resultSet1.next())
                return EnrollResult.ALREADY_ENROLLED;


            //EnrollResult.ALREADY_PASSED
            stmt2.setInt(1, sectionId);
            ResultSet resultSet2 = stmt2.executeQuery();
            while (resultSet2.next())
            {
                int now_grade = resultSet2.getInt("grade");
                if(now_grade >= 60)
                {
                    return EnrollResult.ALREADY_PASSED;
                }
            }


            //EnrollResult.PREREQUISITES_NOT_FULFILLED
            stmt3.setInt(1, sectionId);
            ResultSet resultSet3 = stmt3.executeQuery();
            resultSet3.next();
            //String courseid = resultSet3.getString("courseid");
            if(!passedPrerequisitesForCourse(studentId, courseid))
                return EnrollResult.PREREQUISITES_NOT_FULFILLED;


            //EnrollResult.COURSE_CONFLICT_FOUND
            stmt4.setInt(1, sectionId);
            stmt4.setInt(2, studentId);
            ResultSet resultSet4 = stmt4.executeQuery();
            while (resultSet4.next())
            {
                if(resultSet4.getString("courseid").equals(courseid))
                    return EnrollResult.COURSE_CONFLICT_FOUND;
            }

            stmt5.setInt(1, sectionId);
            ResultSet now_class_resultset = stmt5.executeQuery();
            ArrayList<CourseSectionClass> class_want = new ArrayList<>();
            while (now_class_resultset.next())
            {
                CourseSectionClass courseSectionClass = new CourseSectionClass();
                String day = now_class_resultset.getString("dayOfWeek");
                if(day.equals("MONDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.MONDAY;
                else if(day.equals("TUESDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.TUESDAY;
                else if(day.equals("WEDNESDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.WEDNESDAY;
                else if(day.equals("THURSDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.THURSDAY;
                else if(day.equals("FRIDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.FRIDAY;
                else if(day.equals("SATURDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.SATURDAY;
                else if(day.equals("SUNDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.SUNDAY;
                courseSectionClass.classBegin = now_class_resultset.getShort("classBegin");
                courseSectionClass.classEnd = now_class_resultset.getShort("classEnd");

//                Array week = now_class_resultset.getArray("weeklist");
//                Integer[] week_int = (Integer[])week.getArray();
//                Set<Short> week_short = new HashSet<>();
//                for (Integer integer : week_int)
//                {
//                    week_short.add((short)((int)integer));
//                }
                courseSectionClass.weekList = sql_array_to_set(now_class_resultset.getArray("weeklist"));
                //courseSectionClass.weekList = week_short;
                courseSectionClass.location = now_class_resultset.getString("location");
                class_want.add(courseSectionClass);
            }


            stmt6.setInt(1, studentId);
            stmt6.setInt(2, sectionId);
            ResultSet resultSet6 = stmt6.executeQuery();
            ArrayList<CourseSectionClass> class_have = new ArrayList<>();
            while (resultSet6.next())
            {
                CourseSectionClass courseSectionClass = new CourseSectionClass();
                String day = resultSet6.getString("dayOfWeek");
                if(day.equals("MONDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.MONDAY;
                else if(day.equals("TUESDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.TUESDAY;
                else if(day.equals("WEDNESDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.WEDNESDAY;
                else if(day.equals("THURSDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.THURSDAY;
                else if(day.equals("FRIDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.FRIDAY;
                else if(day.equals("SATURDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.SATURDAY;
                else if(day.equals("SUNDAY"))
                    courseSectionClass.dayOfWeek = DayOfWeek.SUNDAY;
                courseSectionClass.classBegin = resultSet6.getShort("classBegin");
                courseSectionClass.classEnd = resultSet6.getShort("classEnd");
                courseSectionClass.weekList = sql_array_to_set(resultSet6.getArray("weeklist"));
                courseSectionClass.location = resultSet6.getString("location");
                class_have.add(courseSectionClass);
            }

            for (CourseSectionClass this_class : class_want)
            {
                for (CourseSectionClass old_class : class_have)
                {
                    if(this_class.dayOfWeek.equals(old_class.dayOfWeek))
                    {
                        if(!(this_class.classEnd < old_class.classBegin || old_class.classEnd < this_class.classBegin))
                        {
                            for (Short now : this_class.weekList)
                            {
                                if(old_class.weekList.contains(now))
                                    return EnrollResult.COURSE_CONFLICT_FOUND;
                            }
//                            List<Short> weeklist1 = new ArrayList<>();
//                            for (Short now : this_class.weekList)
//                                weeklist1.add(now);
//
//                            List<Short> weeklist2 = new ArrayList<>();
//                            for (Short now : old_class.weekList)
//                                weeklist2.add(now);
                        }
                    }
                }
            }

            //EnrollResult.COURSE_IS_FULL
            {
            stmt7.setInt(1, sectionId);
            ResultSet resultSet7 = stmt7.executeQuery();
            resultSet7.next();
            if(resultSet7.getInt("leftCapacity") <= 0)
                return EnrollResult.COURSE_IS_FULL;
            }

            insert.setInt(1, studentId);
            insert.setInt(2, sectionId);
            insert.setInt(3, -2);
            insert.executeUpdate();
            return EnrollResult.SUCCESS;
        }
        catch (SQLException throwables)
        {
            throwables.printStackTrace();
        }
        return EnrollResult.UNKNOWN_ERROR;
    }

    @Override
    public void dropCourse(int studentId, int sectionId) throws IllegalStateException
    {
        String sql1 = "select grade from student_with_section where studentid = ? and sectionid = ?";
        String sql2 = "delete from student_with_section where studentid = ? and sectionid = ?";
//        String sql3 = "delete from mark_grade where studentid = ? " +
//                "and courseid = (select courseid from coursesection where sectionid = ?)";
//        String sql4 = "delete from pf_grade where studentid = ? " +
//                "and courseid = (select courseid from coursesection where sectionid = ?)";

        try (Connection connection = SQLDataSource.getInstance().getSQLConnection();
             PreparedStatement stmt1 = connection.prepareStatement(sql1);
             PreparedStatement stmt2 = connection.prepareStatement(sql2);
//             PreparedStatement stmt3 = connection.prepareStatement(sql3);
//             PreparedStatement stmt4 = connection.prepareStatement(sql4)
        )
        {
            stmt1.setInt(1, studentId);
            stmt1.setInt(2, sectionId);
            ResultSet resultSet1 = stmt1.executeQuery();
            if(resultSet1.next())
            {
                if(resultSet1.getInt("grade") >= -1)
                    throw new IllegalStateException();
            }

            stmt2.setInt(1, studentId);
            stmt2.setInt(2, sectionId);
            stmt2.executeUpdate();
        }
        catch (SQLException e)
        {
            //e.printStackTrace();
        }
    }

    @Override
    public void addEnrolledCourseWithGrade(int studentId, int sectionId, @Nullable Grade grade)
    {
        String sql0 = "select courseid from coursesection where sectionid = ?";
        String sql1 = "insert into student_with_section (studentid, sectionid, grade) values(?,?,?);";

        try(Connection conn = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt0 = conn.prepareStatement(sql0);
            PreparedStatement stmt1 = conn.prepareStatement(sql1))
        {
            //判断course是否存在
            stmt0.setInt(1, sectionId);
            ResultSet resultSet0 = stmt0.executeQuery();

            if(!resultSet0.next())
                throw new IntegrityViolationException();

            stmt1.setInt(1, studentId);
            stmt1.setInt(2, sectionId);

            if(grade instanceof HundredMarkGrade)
            {
                stmt1.setInt(3, ((HundredMarkGrade) grade).mark);
            }
            else if(grade instanceof PassOrFailGrade)
            {
                if(grade == PassOrFailGrade.PASS)
                {
                    stmt1.setInt(3, 101);
                }
                else if(grade == PassOrFailGrade.FAIL)
                {
                    stmt1.setInt(3, -1);
                }
            }
            else if(grade == null)
            {
                stmt1.setInt(3, -2);
                //-2表示这学期杠选了课，但是还没成绩
            }

            stmt1.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public void setEnrolledCourseGrade(int studentId, int sectionId, Grade grade)
    {
        String sql0 = "select courseid from coursesection where sectionid = ?";
        String sql1 = "update student_with_section set grade = ? where studentid = ? and sectionid = ?;";

        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt0 = connection.prepareStatement(sql0);
            PreparedStatement stmt1 = connection.prepareStatement(sql1))
        {
            //判断course是否存在
            stmt0.setInt(1, sectionId);
            ResultSet resultSet0 = stmt0.executeQuery();

            if(!resultSet0.next())
                throw new IntegrityViolationException();

            stmt1.setInt(2, studentId);
            stmt1.setInt(3, sectionId);

            if(grade instanceof HundredMarkGrade)
            {
                stmt1.setInt(1, ((HundredMarkGrade) grade).mark);
            }
            else if(grade instanceof PassOrFailGrade)
            {
                if(grade == PassOrFailGrade.PASS)
                {
                    stmt1.setInt(1, 101);
                }
                else if(grade == PassOrFailGrade.FAIL)
                {
                    stmt1.setInt(1, -1);
                }
            }

            stmt1.executeUpdate();
        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }

    @Override
    public Map<Course, Grade> getEnrolledCoursesAndGrades(int studentId, @Nullable Integer semesterId)
    {
        Map<Course, Grade> map = new HashMap<>();
        String sql1 = "select grade_number.courseid as courseid,\n" +
                "       grade_number.courseHour as courseHour,\n" +
                "       grade_number.credit as credit,\n" +
                "       grade_number.name as name,\n" +
                "       grade_number.grading as grading,\n" +
                "       grade_number.grade as grade\n" +
                "from (select c.courseid, c.courseHour, c.credit, c.name, c.grading, mg.grade,\n" +
                "       row_number() over (partition by studentid, courseid order by grade desc) as row_number\n" +
                "from mark_grade mg\n" +
                "join course c on c.courseid = mg.courseid\n" +
                "where studentid = ?)grade_number\n" +
                "where grade_number.row_number = 1;";

        String sql2 = "select grade_number.courseid as courseid,\n" +
                "       grade_number.courseHour as courseHour,\n" +
                "       grade_number.credit as credit,\n" +
                "       grade_number.name as name,\n" +
                "       grade_number.grading as grading,\n" +
                "       grade_number.grade as grade\n" +
                "from (select c.courseid, c.courseHour, c.credit, c.name, c.grading, mg.grade,\n" +
                "       row_number() over (partition by studentid, courseid order by grade desc) as row_number\n" +
                "from pf_grade mg\n" +
                "join course c on c.courseid = mg.courseid\n" +
                "where studentid = ?)grade_number\n" +
                "where grade_number.row_number = 1;";
        String sql3 = "select distinct info.courseid as courseid,\n" +
                "                info.courseHour as courseHour,\n" +
                "                info.credit as credit,\n" +
                "                info.name as name ,\n" +
                "                info.grading as grading,\n" +
                "                info.grade as grade\n" +
                "from(select c.courseid, c.courseHour, c.credit, c.name, c.grading, sws.grade,\n" +
                "       row_number() over (partition by studentid, c.courseid order by sws.grade desc ) as row_number\n" +
                "from student_with_section sws\n" +
                "         join courseSection cs on cs.sectionId = sws.sectionid\n" +
                "         join course c on c.courseid = cs.courseid\n" +
                "where cs.semesterId = ? and studentid = ?)info\n" +
                "where row_number = 1;";
        String sql4 = "";
        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2);
            PreparedStatement stmt3 = connection.prepareStatement(sql3);
            PreparedStatement stmt4 = connection.prepareStatement(sql4))
        {
            if(semesterId == null)
            {
                stmt1.setInt(1, studentId);
                ResultSet resultSet1 = stmt1.executeQuery();
                while (resultSet1.next())
                {
                    Course course = new Course();
                    course.id = resultSet1.getString("courseid");
                    course.name = resultSet1.getString("name");
                    course.credit = resultSet1.getInt("credit");
                    course.classHour = resultSet1.getInt("courseHour");
                    int method = resultSet1.getInt("grading");
                    int score = resultSet1.getInt("grade");

                    course.grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                    HundredMarkGrade hundredMarkGrade = new HundredMarkGrade((short) score);
                    map.put(course, hundredMarkGrade);

                }

                stmt2.setInt(1, studentId);
                ResultSet resultSet2 = stmt2.executeQuery();
                while (resultSet2.next())
                {
                    Course course = new Course();
                    course.id = resultSet2.getString("courseid");
                    course.name = resultSet2.getString("name");
                    course.credit = resultSet2.getInt("credit");
                    course.classHour = resultSet2.getInt("courseHour");
                    int method = resultSet2.getInt("grading");
                    int score = resultSet2.getInt("grade");
                    course.grading = Course.CourseGrading.PASS_OR_FAIL;
                    PassOrFailGrade passOrFailGrade;
                    if(score == -1)
                        passOrFailGrade = PassOrFailGrade.PASS;
                    else
                        passOrFailGrade = PassOrFailGrade.FAIL;
                    map.put(course, passOrFailGrade);
                }
            }
            else
            {
                stmt3.setInt(1, semesterId);
                stmt3.setInt(2, studentId);
                ResultSet resultSet = stmt3.executeQuery();
                while (resultSet.next())
                {
                    Course course = new Course();
                    course.id = resultSet.getString("courseid");
                    course.name = resultSet.getString("name");
                    course.credit = resultSet.getInt("credit");
                    course.classHour = resultSet.getInt("courseHour");
                    int method = resultSet.getInt("grading");
                    int score = resultSet.getInt("grade");
                    if(method == 2)
                    {
                        course.grading = Course.CourseGrading.PASS_OR_FAIL;
                        PassOrFailGrade passOrFailGrade;
                        if(score == -1)
                            passOrFailGrade = PassOrFailGrade.PASS;
                        else
                            passOrFailGrade = PassOrFailGrade.FAIL;
                        map.put(course, passOrFailGrade);
                    }
                    else
                    {
                        course.grading = Course.CourseGrading.HUNDRED_MARK_SCORE;
                        HundredMarkGrade hundredMarkGrade = new HundredMarkGrade((short) score);
                        map.put(course, hundredMarkGrade);
                    }
                }
            }
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }
        return map;
    }

    @Override
    public CourseTable getCourseTable(int studentId, Date date) {
        String sql = "select ceiling((?-(select s.beginTime  from semester s where ? between s.beginTime and s.endTime)+0.0)/(7+0.0)) as teach_week";
        int teach_week;
        try(Connection conn = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement ps = conn.prepareStatement(sql))
        {
            ps.setDate(1,date);
            ps.setDate(2,date);

            ResultSet rss = ps.executeQuery();
            teach_week=rss.getInt("teach_week");
        }
        catch (SQLException e)
        {
            throw new EntityNotFoundException();
        }

        CourseTable coursetable = new CourseTable();
        Set<CourseTable.CourseTableEntry> MONDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> TUESDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> WEDNESDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> THURSDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> FRIDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> SATURDAY = new HashSet<>();
        Set<CourseTable.CourseTableEntry> SUNDAY = new HashSet<>();


        String sql2 = "select cs.dayOfWeek,\n" +
                "       c2.name||'['||c1.name||']' as class_name,\n" +
                "       i.fullname,\n" +
                "       cs.classBegin,\n" +
                "       cs.classEnd,\n" +
                "       cs.location\n" +
                "from student_with_section s\n" +
                "join coursesectionclass cs\n" +
                "    on cs.sectionid=s.sectionid\n" +
                "join instructor i\n" +
                "    on cs.instructorId = i.instructorId\n" +
                "join coursesection c1\n" +
                "    on c1.sectionid=s.sectionid\n" +
                "join course c2\n" +
                "    on c2.courseid=c1.courseId\n" +
                "where s.studentid=? and ? =ANY(cs.weekList);";
        try(Connection conn2 = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement ps2 = conn2.prepareStatement(sql2))
        {
            ps2.setInt(1,studentId);
            ps2.setInt(2,teach_week);
            ResultSet rss2 = ps2.executeQuery();
            while (rss2.next()){
                //CStable.table.put(DayOfWeek.FRIDAY,);
                CourseTable cstable=new CourseTable();
                //CourseTable.CourseTableEntry.instructor=rss2.getString("i.fullname");
                CourseTable.CourseTableEntry cs=new CourseTable.CourseTableEntry();

                String dayofweek=rss2.getString("cs.dayOfWeek");

                cs.courseFullName=rss2.getString("class_name");
                cs.instructor.fullName=rss2.getString("i.fullname");
                cs.classBegin=rss2.getShort("cs.classBegin");
                cs.classEnd=rss2.getShort("cs.classEnd");
                cs.location=rss2.getString("cs.location");
                if(dayofweek.equals("MONDAY")){
                    //coursetable.table.put(DayOfWeek.MONDAY,)
                    MONDAY.add(cs);
                }
                else if(dayofweek.equals("TUESDAY")){
                    TUESDAY.add(cs);
                }
                else if(dayofweek.equals("WEDNESDAY")){
                    WEDNESDAY.add(cs);
                }
                else if(dayofweek.equals("THURSDAY")){
                    THURSDAY.add(cs);
                }
                else if(dayofweek.equals("FRIDAY")){
                    FRIDAY.add(cs);
                }
                else if(dayofweek.equals("SATURDAY")){
                    SATURDAY.add(cs);
                }
                else if(dayofweek.equals("SUNDAY")){
                    SUNDAY.add(cs);
                }
            }
            coursetable.table.put(DayOfWeek.MONDAY,MONDAY);
            coursetable.table.put(DayOfWeek.TUESDAY,TUESDAY);
            coursetable.table.put(DayOfWeek.WEDNESDAY,WEDNESDAY);
            coursetable.table.put(DayOfWeek.THURSDAY,THURSDAY);
            coursetable.table.put(DayOfWeek.FRIDAY,FRIDAY);
            coursetable.table.put(DayOfWeek.SATURDAY,SATURDAY);
            coursetable.table.put(DayOfWeek.SUNDAY,SUNDAY);
        }
        catch (SQLException e2)
        {
            throw new EntityNotFoundException();
        }
        return coursetable;
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
            if(strings[i].length() == 1 || strings[i].length() == 2)
                numbers[i] = Integer.valueOf(strings[i]);
            else
                numbers[i] = Integer.valueOf(strings[i].substring(1, strings[i].length() - 1));
        }
        return numbers;
    }

    public static Set<Short> sql_array_to_set(Array array) throws SQLException
    {
        //Array week = now_class_resultset.getArray("weeklist");
        Integer[] week_int = (Integer[])array.getArray();
        Set<Short> week_short = new HashSet<>();
        for (Integer integer : week_int)
        {
            week_short.add((short)((int)integer));
        }

        return week_short;
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
