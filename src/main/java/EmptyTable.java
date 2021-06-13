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

public class EmptyTable
{
    public static void main(String[] args)
    {
        String sql = "drop table mark_grade;\n" +
                "drop table pf_grade;\n" +
                "drop table student_with_section;\n" +
                "drop table courseSectionClass;\n" +
                "drop table instructor;\n" +
                "drop table courseSection;\n" +
                "drop table semester;\n" +
                "drop table prerequisite;\n" +
                "drop table student;\n" +
                "drop table course_with_major;\n" +
                "drop table course;\n" +
                "drop table major;\n" +
                "drop table users;\n" +
                "drop table department;";

        String sql1 = "create table department\n" +
                "(\n" +
                "    id                  integer\n" +
                "        constraint department_pkey\n" +
                "            primary key ,\n" +
                "    name                varchar,\n" +
                "    constraint department_id_name_key\n" +
                "        unique (id, name)\n" +
                ");\n" +
                "\n" +
                "create table major\n" +
                "(\n" +
                "    majorId             integer\n" +
                "        constraint major_pkey\n" +
                "            primary key ,\n" +
                "    name                varchar,\n" +
                "    department          integer\n" +
                "        constraint major_fkey\n" +
                "            references department(id) on delete cascade ,\n" +
                "    constraint major_whole_key\n" +
                "        unique (majorId, name, department)\n" +
                ");\n" +
                "\n" +
                "create table users\n" +
                "(\n" +
                "    userId                  integer\n" +
                "        constraint user_pkey\n" +
                "            primary key ,\n" +
                "    character           integer,\n" +
                "    constraint user_key1\n" +
                "        unique (userId, character)\n" +
                ");\n" +
                "\n" +
                "create table instructor\n" +
                "(\n" +
                "    instructorId        integer\n" +
                "        constraint instructor_pkey\n" +
                "            primary key\n" +
                "        constraint instructor_fkey\n" +
                "            references users(userId) on delete cascade ,\n" +
                "    firstname           varchar,\n" +
                "    lastname            varchar\n" +
                ");\n" +
                "\n" +
                "create table student\n" +
                "(\n" +
                "    studentId           integer\n" +
                "        constraint student_pkey\n" +
                "            primary key\n" +
                "        constraint student_fkey_1\n" +
                "            references users(userId) on delete cascade ,\n" +
                "    firstname           varchar,\n" +
                "    lastname            varchar,\n" +
                "    enrolledDate        date,\n" +
                "    major               integer\n" +
                "        constraint student_fkey\n" +
                "            references major(majorId) on delete cascade\n" +
                ");\n" +
                "\n" +
                "create table semester\n" +
                "(\n" +
                "    id                  integer\n" +
                "        constraint semester_pkey\n" +
                "            primary key ,\n" +
                "    name                varchar,\n" +
                "    beginTime           date,\n" +
                "    endTime             date\n" +
                ");\n" +
                "\n" +
                "create table course\n" +
                "(\n" +
                "    courseid            varchar not null\n" +
                "        constraint course_pkey\n" +
                "            primary key,\n" +
                "    courseHour          integer,\n" +
                "    credit              integer,\n" +
                "    name                varchar not null,\n" +
                "    grading             integer,\n" +
                "    constraint course_courseid_coursename_key\n" +
                "        unique (courseid, name)\n" +
                ");\n" +
                "\n" +
                "create table courseSection\n" +
                "(\n" +
                "    courseId            varchar not null\n" +
                "        constraint courseSection_fkey\n" +
                "            references course(courseid) on delete cascade ,\n" +
                "    sectionId           integer not null\n" +
                "        constraint section_pkey\n" +
                "            primary key ,\n" +
                "    semesterId          integer not null\n" +
                "        constraint section_semster_fkey\n" +
                "            references semester(id) on delete cascade ,\n" +
                "    name                varchar,\n" +
                "    totalCapacity       integer,\n" +
                "    leftCapacity        integer,\n" +
                "    constraint Section_key\n" +
                "        unique (sectionId, name)\n" +
                ");\n" +
                "\n" +
                "create table courseSectionClass\n" +
                "(\n" +
                "    sectionId           integer not null\n" +
                "        constraint class_fkey\n" +
                "            references courseSection(sectionId) on delete cascade ,\n" +
                "    classId             integer not null\n" +
                "        constraint class_pkey\n" +
                "            primary key,\n" +
                "    instructorId        integer not null\n" +
                "        constraint class_fkey_3\n" +
                "            references instructor(instructorId) on delete cascade ,\n" +
                "    dayOfWeek           varchar,\n" +
                "    weekList            integer[],\n" +
                "    classBegin          integer,\n" +
                "    classEnd            integer,\n" +
                "    location            varchar,\n" +
                "    constraint class_ukey1\n" +
                "        unique (sectionId, instructorId, dayOfWeek, weekList, classBegin, classEnd)\n" +
                ");\n" +
                "\n" +
                "create table student_with_section\n" +
                "(\n" +
                "    studentid           integer not null\n" +
                "        constraint student_with_section_fkey1\n" +
                "            references student(studentId) on delete cascade ,\n" +
                "    sectionid           integer not null\n" +
                "        constraint student_with_section_fkey2\n" +
                "            references courseSection(sectionid) on delete cascade ,\n" +
                "    grade               integer,\n" +
                "    constraint student_with_section_pkey\n" +
                "        primary key (studentid, sectionid)\n" +
                ");\n" +
                "\n" +
                "create table course_with_major\n" +
                "(\n" +
                "    courseId            varchar\n" +
                "        constraint course_with_major_fkey1\n" +
                "            references course(courseid) on delete cascade ,\n" +
                "    majorId             integer\n" +
                "        constraint course_with_major_fkey2\n" +
                "            references major(majorId) on delete cascade ,\n" +
                "    courseTypeInMajor   integer,\n" +
                "    constraint course_with_major_key\n" +
                "        unique (courseId, majorId)\n" +
                ");\n" +
                "\n" +
                "create table prerequisite\n" +
                "(\n" +
                "    courseid varchar\n" +
                "        constraint prerequisite_fkey\n" +
                "            references course\n" +
                "        on delete cascade ,\n" +
                "    index    integer,\n" +
                "    relation varchar,\n" +
                "    child    integer[]\n" +
                ");\n" +
                "\n" +
                "create table mark_grade\n" +
                "(\n" +
                "    studentid integer not null\n" +
                "        constraint student_with_section_fkey1\n" +
                "            references student\n" +
                "            on delete cascade,\n" +
                "    courseid  varchar not null\n" +
                "        constraint coursesection_fkey\n" +
                "            references course\n" +
                "            on delete cascade,\n" +
                "    grade     integer\n" +
                ");\n" +
                "\n" +
                "create table pf_grade\n" +
                "(\n" +
                "    studentid integer not null\n" +
                "        constraint student_with_section_fkey1\n" +
                "            references student\n" +
                "            on delete cascade,\n" +
                "    courseid  varchar not null\n" +
                "        constraint coursesection_fkey\n" +
                "            references course\n" +
                "            on delete cascade,\n" +
                "    grade     varchar\n" +
                ");";

        String sql2 = "create or replace function insert_department_check()\n" +
                "    returns trigger\n" +
                "as\n" +
                "$$\n" +
                "begin\n" +
                "    new.id = (select coalesce(max(id), 0) as numebr from department) + 1;\n" +
                "    return new;\n" +
                "-- write your code here\n" +
                "end\n" +
                "$$ language plpgsql;\n" +
                "\n" +
                "create trigger insert_department_trigger\n" +
                "    before\n" +
                "        insert\n" +
                "    on department\n" +
                "    for each row\n" +
                "execute procedure insert_department_check();\n" +
                "\n" +
                "\n" +
                "--addMajor时的trigger\n" +
                "create or replace function add_major_check()\n" +
                "    returns trigger\n" +
                "as\n" +
                "$$\n" +
                "begin\n" +
                "    new.majorid = ((select coalesce(max(majorid), 0) from major) + 1);\n" +
                "    return new;\n" +
                "-- write your code here\n" +
                "end\n" +
                "$$ language plpgsql;\n" +
                "\n" +
                "create trigger add_major_trigger\n" +
                "    before\n" +
                "        insert\n" +
                "    on major\n" +
                "    for each row\n" +
                "execute procedure add_major_check();\n" +
                "\n" +
                "--add semester的时候加的\n" +
                "create or replace function add_semester_check()\n" +
                "    returns trigger\n" +
                "as\n" +
                "$$\n" +
                "begin\n" +
                "    new.id = (select coalesce(max(id), 0) from semester) + 1;\n" +
                "    return new;\n" +
                "-- write your code here\n" +
                "end\n" +
                "$$ language plpgsql;\n" +
                "\n" +
                "create trigger add_semester_trigger\n" +
                "    before\n" +
                "        insert\n" +
                "    on semester\n" +
                "    for each row\n" +
                "execute procedure add_semester_check();\n" +
                "\n" +
                "\n" +
                "--addCourseSection用的\n" +
                "create or replace function add_coursesection_check()\n" +
                "    returns trigger\n" +
                "as\n" +
                "$$\n" +
                "declare\n" +
                "    new_id integer;\n" +
                "begin\n" +
                "    --new_id := 0;\n" +
                "    new_id := (select coalesce(max(sectionid), 0) from coursesection) + 1;\n" +
                "    new.sectionid = new_id;\n" +
                "    return new;\n" +
                "end\n" +
                "$$ language plpgsql;\n" +
                "\n" +
                "create trigger add_coursesection_trigger\n" +
                "    before\n" +
                "        insert\n" +
                "    on coursesection\n" +
                "    for each row\n" +
                "execute procedure add_coursesection_check();\n" +
                "\n" +
                "--addCourseSectionClass用的\n" +
                "create or replace function add_coursesectionClass_check()\n" +
                "    returns trigger\n" +
                "as\n" +
                "$$\n" +
                "begin\n" +
                "    new.classid = (select coalesce(max(classid), 0) from coursesectionclass) + 1;\n" +
                "    return new;\n" +
                "end\n" +
                "$$ language plpgsql;\n" +
                "\n" +
                "create trigger add_coursesectionClass_trigger\n" +
                "    before\n" +
                "        insert\n" +
                "    on coursesectionClass\n" +
                "    for each row\n" +
                "execute procedure add_coursesectionClass_check();\n" +
                "\n" +
                "--获取名字时用的\n" +
                "create or replace function full_name(firstname varchar, lastname varchar)\n" +
                "returns varchar\n" +
                "as\n" +
                "$$\n" +
                "begin\n" +
                "    if firstname ~ '[a-z]' or firstname ~ '[A-Z]'\n" +
                "           or lastname ~ '[a-z]' or lastname ~ '[A-Z]'\n" +
                "    then\n" +
                "        return firstname || ' ' || lastname;\n" +
                "    else\n" +
                "        return firstname || lastname;\n" +
                "    end if;\n" +
                "end;\n" +
                "$$ language plpgsql;";

        try(Connection connection = SQLDataSource.getInstance().getSQLConnection();
            PreparedStatement stmt = connection.prepareStatement(sql);
            PreparedStatement stmt1 = connection.prepareStatement(sql1);
            PreparedStatement stmt2 = connection.prepareStatement(sql2))
        {
            stmt.executeUpdate();

//            ResultSet resultSet = stmt1.executeQuery();
//            if(!resultSet.next())
//                throw new EntityNotFoundException();

            stmt1.executeUpdate();
            stmt2.executeUpdate();

        }
        catch (SQLException e)
        {
            e.printStackTrace();
        }
    }
}
