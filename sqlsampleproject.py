# Databricks notebook source
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS emp;
# MAGIC

# COMMAND ----------

dbutils.fs.rm("dbfs:/user/hive/warehouse/emp", recurse=True)


# COMMAND ----------

# MAGIC   %sql
# MAGIC create table emp(empno int, ename string,
# MAGIC job string,
# MAGIC mgr int,
# MAGIC hiredate date,
# MAGIC sal int,
# MAGIC comm int,
# MAGIC deptno int,
# MAGIC updated_Date date) 

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO emp (empno, ename, job, mgr, hiredate, sal, comm, deptno, updated_Date) VALUES
# MAGIC (1, 'John', 'Manager', NULL, '2020-01-15', 60000, 5000, 10, '2024-03-01'),
# MAGIC (2, 'Alice', 'Analyst', 1, '2021-06-20', 55000, NULL, 20, '2024-03-02'),
# MAGIC (3, 'Bob', 'Clerk', 2, '2022-09-10', 30000, 2000, 30, '2024-03-03'),
# MAGIC (4, 'David', 'Salesman', 1, '2019-12-05', 45000, 3000, 10, NULL),
# MAGIC (5, 'Emma', 'Manager', NULL, '2018-07-25', 70000, NULL, 20, '2024-03-05'),
# MAGIC (6, 'Michael', 'Analyst', 5, '2020-11-10', NULL, 4000, 10, '2024-03-06'),
# MAGIC (7, 'Sara', 'Clerk', 6, '2021-03-18', 28000, NULL, 30, NULL),
# MAGIC (8, 'James', 'Salesman', 1, '2017-08-30', 42000, 2500, 10, '2024-03-08'),
# MAGIC (9, 'Olivia', 'Analyst', 5, '2019-05-22', 58000, NULL, 20, '2024-03-09'),
# MAGIC (10, 'Ethan', 'Clerk', 2, '2022-10-01', NULL, 1000, 30, NULL),
# MAGIC (11, 'John', 'Manager', NULL, '2020-01-15', 60000, NULL, 10, '2024-03-11'),
# MAGIC (12, 'Alice', 'Analyst', 1, '2021-06-20', 55000, NULL, 20, '2024-03-12'),
# MAGIC (13, 'David', 'Salesman', 1, '2019-12-05', 45000, 3000, 10, NULL),
# MAGIC (14, 'Emma', 'Manager', NULL, '2018-07-25', 70000, NULL, 20, '2024-03-14'),
# MAGIC (15, 'Michael', 'Analyst', 5, '2020-11-10', NULL, 4000, 10, '2024-03-15'),
# MAGIC (16, 'Sara', 'Clerk', 6, '2021-03-18', 28000, NULL, 30, NULL),
# MAGIC (17, 'James', 'Salesman', 1, '2017-08-30', 42000, 2500, 10, '2024-03-17'),
# MAGIC (18, 'Olivia', 'Analyst', 5, '2019-05-22', 58000, NULL, 20, '2024-03-18'),
# MAGIC (19, 'Ethan', 'Clerk', 2, '2022-10-01', NULL, 1000, 30, NULL),
# MAGIC (20, 'Liam', 'Salesman', 1, '2018-06-12', 50000, 3000, 20, NULL),
# MAGIC (21, 'Sophia', 'Analyst', 3, '2019-09-21', NULL, NULL, 10, '2024-03-21'),
# MAGIC (22, 'Daniel', 'Manager', NULL, '2016-12-14', 75000, 5000, 20, '2024-03-22'),
# MAGIC (23, 'Grace', 'Clerk', 7, '2021-11-05', 32000, NULL, 30, NULL),
# MAGIC (24, 'Ryan', 'Salesman', 4, '2020-03-14', 47000, 2000, 10, '2024-03-24'),
# MAGIC (25, 'Ava', 'Analyst', 8, '2021-08-18', 56000, 1500, 20, '2024-03-25'),
# MAGIC (26, 'Noah', 'Manager', NULL, '2017-02-28', 80000, NULL, 30, '2024-03-26'),
# MAGIC (27, 'Mia', 'Clerk', 9, '2023-01-10', 29000, 1200, 10, NULL),
# MAGIC (28, 'Lucas', 'Salesman', 6, '2018-10-20', 46000, 2500, 20, '2024-03-28'),
# MAGIC (29, 'Harper', 'Analyst', 5, '2020-05-30', 54000, NULL, 30, '2024-03-29'),
# MAGIC (30, 'Benjamin', 'Clerk', 10, '2021-07-15', NULL, 900, 10, NULL);
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC what ae the jobs are presented in above data set

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct job
# MAGIC from emp;

# COMMAND ----------

# MAGIC %md 
# MAGIC i want to know the employee name and job title who are earning more than 6000
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select  ename,job,sal
# MAGIC from emp
# MAGIC where sal >= 60000;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct ename,job,sal
# MAGIC from emp
# MAGIC where sal >60000;

# COMMAND ----------

# MAGIC %md
# MAGIC user want to update a new record i.e 3 row as teja with the role of analyst
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC update emp
# MAGIC set ename="teja", job='analyst'
# MAGIC where empno=3; 

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;

# COMMAND ----------

# MAGIC %md 
# MAGIC Now calculate  experience
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select hiredate,date_format(coalesce(updated_date,current_date()),"m-dd-yyyy") as updated
# MAGIC from emp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT empno, ename, job, hiredate, updated_Date, 
# MAGIC        DATEDIFF(YEAR, hiredate, COALESCE(updated_Date, GETDATE())) AS experience_years
# MAGIC FROM emp;
# MAGIC

# COMMAND ----------

# MAGIC %md 
# MAGIC whose exp > 5 consider senior , btwn 2 and  5 junior ,<2 fresher

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ename, job,
# MAGIC        CASE 
# MAGIC            WHEN (YEAR(COALESCE(updated_Date, GETDATE())) - YEAR(hiredate)) > 5 THEN 'Senior'
# MAGIC            WHEN (YEAR(COALESCE(updated_Date, GETDATE())) - YEAR(hiredate)) BETWEEN 2 AND 5 THEN 'Junior'
# MAGIC            WHEN (YEAR(COALESCE(updated_Date, GETDATE())) - YEAR(hiredate)) < 2 THEN 'Fresher'
# MAGIC            ELSE 'Unknown'
# MAGIC        END AS experience_level
# MAGIC FROM emp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct empno, ename, job, mgr, hiredate, sal, comm, deptno, updated_Date
# MAGIC from emp;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC i want to insert 31 record and delete
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC
# MAGIC
# MAGIC insert into emp(empno, ename, job, mgr, hiredate, sal, comm, deptno, updated_Date) 
# MAGIC values(31,'raj', 'analyst', 5, '2020-11-10', NULL, 4000, 10, '2024-03-15')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC delete from emp
# MAGIC where empno=31;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;

# COMMAND ----------

# MAGIC %md i want to know the second highest sal

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select ename,job,sal
# MAGIC from emp
# MAGIC order by sal desc
# MAGIC limit 3;

# COMMAND ----------

# MAGIC %md want to findout maximum salary as per job

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT job, MAX(sal) AS salary
# MAGIC FROM emp
# MAGIC GROUP BY job
# MAGIC ORDER BY salary DESC;
# MAGIC

# COMMAND ----------

# MAGIC %md change date format
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select  ename,
# MAGIC job,
# MAGIC date_format(to_date(updated_date, 'yyyy-MM-dd'), 'MM-dd-yyyy') AS updated
# MAGIC
# MAGIC from emp;

# COMMAND ----------

# MAGIC %sql 
# MAGIC select distinct deptno from emp;

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from emp;

# COMMAND ----------



# COMMAND ----------

