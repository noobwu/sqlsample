/*
CREATE table dbo.DataRows
(    ID int not null,
    Col1 varchar(255) null,
    Col2 varchar(255) null,
    Col3 varchar(255) null
);
insert into dbo.DataRows(ID, Col1, Col3)  values (1,replicate('a',10),replicate('c',10));
insert into dbo.DataRows(ID, Col2) values (2,replicate('b',10));
*/

/*
dbcc ind
(    'SQLServerInternals' /*Database Name*/
    ,'dbo.DataRows' /*Table Name*/
    ,-1 /*Display information for all pages of all indexes*/
);


-- Redirecting DBCC PAGE output to console
dbcc traceon(3604);
dbcc page
(    
  'SqlServerInternals' /*Database Name*/
  ,1 /*File ID*/
  ,280 /*Page ID*/
  ,3 /*Output mode: 3 - display page header and row details */
);
*/

/*

create table dbo.BadTable
(    
  Col1 char(4000),
  Col2 char(4060)
)
*/

/*
CREATE TABLE dbo.AlterDemo
(
    ID INT NOT NULL,
    Col1 INT NULL,
    Col2 BIGINT NULL,
    Col3 CHAR(10) NULL,
    Col4 TINYINT NULL
);


SELECT c.column_id,c.name,
       ipc.leaf_offset AS [Offset in Row],
       ipc.max_inrow_length AS [Max Length],
       ipc.system_type_id AS [Column Type]
FROM sys.system_internals_partition_columns ipc
    JOIN sys.partitions p
        ON ipc.partition_id = p.partition_id
    JOIN sys.columns c
        ON c.column_id = ipc.partition_column_id
           AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID(N'dbo.AlterDemo')
ORDER BY c.column_id;

*/

/*
ALTER TABLE dbo.AlterDemo DROP COLUMN Col1;
ALTER TABLE dbo.AlterDemo ALTER COLUMN Col2 TINYINT;
ALTER TABLE dbo.AlterDemo ALTER COLUMN Col3 CHAR(1);
ALTER TABLE dbo.AlterDemo ALTER COLUMN Col4 INT;

SELECT c.column_id,c.name,
       ipc.leaf_offset AS [Offset in Row],
       ipc.max_inrow_length AS [Max Length],
       ipc.system_type_id AS [Column Type]
FROM sys.system_internals_partition_columns ipc
    JOIN sys.partitions p
        ON ipc.partition_id = p.partition_id
    JOIN sys.columns c
        ON c.column_id = ipc.partition_column_id
           AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID(N'dbo.AlterDemo')
ORDER BY c.column_id;

*/


/*

ALTER TABLE dbo.AlterDemo REBUILD

SELECT c.column_id,c.name,
       ipc.leaf_offset AS [Offset in Row],
       ipc.max_inrow_length AS [Max Length],
       ipc.system_type_id AS [Column Type]
FROM sys.system_internals_partition_columns ipc
    JOIN sys.partitions p
        ON ipc.partition_id = p.partition_id
    JOIN sys.columns c
        ON c.column_id = ipc.partition_column_id
           AND c.object_id = p.object_id
WHERE p.object_id = OBJECT_ID(N'dbo.AlterDemo')
ORDER BY c.column_id;

*/

/* 数据行大小
CREATE TABLE dbo.LargeRows
(
    ID INT NOT NULL,
    Col CHAR(2000) NULL
);
CREATE TABLE dbo.SmallRows
(
    ID INT NOT NULL,
    Col VARCHAR(2000) NULL
);
;with N1(C) as (
SELECT 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) --65,536 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)
insert into dbo.LargeRows(ID, Col)
    select ID, 'Placeholder' from Ids;
insert into dbo.SmallRows(ID, Col)
    select ID, 'Placeholder' from dbo.LargeRows;



SET STATISTICS IO ON 
SET STATISTICS TIME ON

select count(*) from dbo.LargeRows;
select count(*) from dbo.SmallRows;

SET STATISTICS TIME OFF
SET STATISTICS IO OFF

*/


/*
 SELECT * and I/O

create table dbo.Employees
(    
  EmployeeId int not null,
  Name varchar(128) not null,
  Picture varbinary(max) null
);
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N2 as T2) -- 1,024 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)
insert into dbo.Employees(EmployeeId, Name, Picture)
select ID, 'Employee ' + convert(varchar(5),ID),convert(varbinary(max),replicate(convert(varchar(max),'a'),120000)) from Ids;

*/

/*
SET STATISTICS IO ON 
SET STATISTICS TIME ON 

SELECT * FROM dbo.Employees;
SELECT EmployeeId,Name FROM dbo.Employees;

SET STATISTICS IO OFF
SET STATISTICS TIME OFF

*/

/*
SET STATISTICS  XML ON

CREATE TABLE dbo.HalloweenProtection
(
    Id INT NOT NULL IDENTITY(1, 1),
    Data INT NOT NULL
);

INSERT INTO dbo.HalloweenProtection(Data)
SELECT Data FROM dbo.HalloweenProtection;

SET STATISTICS  XML OFF

*/

/*

CREATE FUNCTION dbo.ShouldUpdateData
(
    @Id INT
)
RETURNS BIT
AS
BEGIN
    RETURN (1);
END;
GO 

CREATE FUNCTION dbo.ShouldUpdateDataSchemaBound
(
    @Id int
)
RETURNS bit
WITH SCHEMABINDING
AS
BEGIN
    RETURN (1);
END;

GO 


SET STATISTICS XML ON

update dbo.HalloweenProtection set Data = 0 where dbo.ShouldUpdateData(ID) = 1;
update dbo.HalloweenProtection set Data = 0 where dbo.ShouldUpdateDataSchemaBound(ID) = 1;

SET STATISTICS XML OFF

*/


/*
CREATE TABLE dbo.T1
(
    T1ID INT NOT NULL,
    Placeholder CHAR(100),
    CONSTRAINT PK_T1
        PRIMARY KEY CLUSTERED (T1ID)
);
CREATE TABLE dbo.T2
(
    T1ID INT NOT NULL,
    T2ID INT NOT NULL,
    Placeholder CHAR(100)
);


create unique clustered index IDX_T2_T1ID_T2ID
on dbo.T2(T1ID, T2ID);
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,Nums(Num) as (select row_number() over (order by (select null)) from N5)
insert into dbo.T1(T1ID)
    select Num from Nums;
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,Nums(Num) as (select row_number() over (order by (select null)) from N3)
insert into dbo.T2(T1ID, T2ID)
    select T1ID, Num from dbo.T1 cross join Nums;

*/


/*
SET STATISTICS XML ON

select count(*)
from
    (
        select t1.T1ID, count(*) as Cnt
        from dbo.T1 t1 join dbo.T2 t2 on
            t1.T1ID = t2.T1ID
        group by t1.T1ID
    ) s
option (maxdop 1);
select count(*)
from
    (
        select t1.T1ID, count(*) as Cnt
        from dbo.T1 t1 join dbo.T2 t2 on
            t1.T1ID = t2.T1ID
        group by t1.T1ID
    ) s;

SET STATISTICS XML OFF

*/

/*
create table dbo.Books
(
	BookId int identity(1,1) not null,
	Title nvarchar(256) not null,
	-- International Standard Book Number
	ISBN char(14) not null,
	Placeholder char(150) null
);
create unique clustered index IDX_Books_BookId on dbo.Books(BookId);

-- 1,252,000 rows
;with Prefix(Prefix)
as
(    select 100
    union all
    select Prefix + 1
    from Prefix
    where Prefix < 600
)
,Postfix(Postfix)
as
(    select 100000001
    union all
    select Postfix + 1
    from Postfix
    where Postfix < 100002500
)

insert into dbo.Books(ISBN, Title)
    select
        convert(char(3), Prefix) + '-0' + convert(char(9),Postfix)
        ,'Title for ISBN' + convert(char(3), Prefix) + '-0' + convert(char(9),Postfix)
    from Prefix cross join Postfix
option (maxrecursion 0);
create nonclustered index IDX_Books_ISBN on dbo.Books(ISBN);

*/


-- DBCC SHOW_STATISTICS('dbo.Books',IDX_BOOKS_ISBN)

/*

;with Prefix(Prefix)
as ( select Num from (values(104),(104),(104),(104),(104)) Num(Num) )
,Postfix(Postfix)
as
(
  select 100000001
  union all
  select Postfix + 1 from Postfix where Postfix < 100002500
)
insert into dbo.Books(ISBN, Title)
select
convert(char(3), Prefix) + '-0' + convert(char(9),Postfix)
,'Title for ISBN' + convert(char(3), Prefix) + '-0' + convert(char(9),Postfix)
from Prefix cross join Postfix
option (maxrecursion 0);

-- Updating the statistics
update statistics dbo.Books IDX_Books_ISBN with fullscan;
*/

/*
DBCC SHOW_STATISTICS ('dbo.Books',IDX_BOOKS_ISBN ) 
*/

/*
SET STATISTICS PROFILE ON
SET STATISTICS XML ON 

SELECT BookId, Title FROM dbo.Books WHERE ISBN LIKE '114%'

SET STATISTICS PROFILE OFF
SET STATISTICS XML OFF

*/

/*
create table dbo.Customers
(
	CustomerId int not null identity(1,1),
	FirstName nvarchar(64) not null,
	LastName nvarchar(128) not null,
	Phone varchar(32) null,
	Placeholder char(200) null
);
create unique clustered index IDX_Customers_CustomerId ON dbo.Customers(CustomerId)
go
-- Inserting cross-joined data for all first and last names 50 times
-- using GO 50 command in Management Studio
;with FirstNames(FirstName)
as
(
	select Names.Name
	from ( values('Andrew'),('Andy'),('Anton'),('Ashley'),('Boris'),('Brian'),
	('Cristopher'),('Cathy')
	, ('Daniel'),('Donny'),('Edward'),('Eddy'),('Emy'),('Frank'),('George'),
	('Harry'),('Henry')
	, ('Ida'),('John'),('Jimmy'),('Jenny'),('Jack'),('Kathy'),('Kim'),('Larry'),
	('Mary'),('Max')
	, ('Nancy'),('Olivia'),('Olga'),('Peter'),('Patrick'),('Robert'),('Ron'),
	('Steve'),('Shawn')
	,('Tom'),('Timothy'),('Uri'),('Vincent') ) Names(Name)
)
,LastNames(LastName)
as
(
	select Names.Name
	from ( values('Smith'),('Johnson'),('Williams'),('Jones'),('Brown'),('Davis'),('Miller')
	,('Wilson'), ('Moore'),('Taylor'),('Anderson'),('Jackson'),('White'),('Harris') )
	Names(Name)
)

insert into dbo.Customers(LastName, FirstName)
select LastName, FirstName from FirstNames cross join LastNames
go 50

insert into dbo.Customers(LastName, FirstName) values('Isakov','Victor')
GO

create nonclustered index IDX_Customers_LastName_FirstName ON dbo.Customers(LastName, FirstName);

*/

/*

--SET STATISTICS PROFILE ON
--SET STATISTICS XML ON


SELECT CustomerId, FirstName,LastName,Phone
FROM dbo.Customers
WHERE FirstName = 'Brian';

SELECT CustomerId,FirstName, LastName,Phone
FROM dbo.Customers
WHERE FirstName = 'Victor';


--SET STATISTICS XML OFF
--SET STATISTICS PROFILE OFF

*/

/*
select  stats_id, name, auto_created
from sys.stats
where object_id = object_id(N'dbo.Customers')


DBCC SHOW_STATISTICS ('dbo.Customers', _WA_Sys_00000002_6EF57B66)

*/


/*
;with Postfix(Postfix)
as
(
	select 100000001
	union all
	select Postfix + 1
	from Postfix
	where Postfix < 100250000
)
insert into dbo.Books(ISBN, Title)
SELECT  '999-0' + convert(char(9),Postfix),'Title for ISBN 999-0' + convert(char(9),Postfix)
FROM Postfix
OPTION (maxrecursion 0);
*/

/*
SELECT * FROM dbo.Books WHERE ISBN LIKE '999%'
*/

/*
DBCC SHOW_STATISTICS ('dbo.Books', IDX_BOOKS_ISBN)

UPDATE STATISTICS dbo.Books IDX_Books_ISBN WITH FULLSCAN

*/


/*
create table dbo.MemoryGrantDemo
(
	ID int not null,
	Col int not null,
	Placeholder char(8000)
);
create unique clustered index IDX_MemoryGrantDemo_ID ON dbo.MemoryGrantDemo(ID);


;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)
insert into dbo.MemoryGrantDemo(ID,Col,Placeholder)
select ID, ID % 100, convert(char(100),ID) from IDs;

create nonclustered index IDX_MemoryGrantDemo_Col ON dbo.MemoryGrantDemo(Col);

*/



/*
;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N2 as T2) -- 1,024 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)

insert into dbo.MemoryGrantDemo(ID,Col,Placeholder)
    select 100000 + ID, 1000, convert(char(100),ID)
    from IDs
    where ID <= 656;
	
*/

/*

DECLARE @Dummy int
set statistics time on
select @Dummy = ID from dbo.MemoryGrantDemo where Col = 1 order by Placeholder;
select @Dummy = ID from dbo.MemoryGrantDemo where Col = 1000 order by Placeholder;
set statistics time off

*/

select
    s.stats_id as [Stat ID], sc.name + '.' + t.name as [Table], s.name as [Statistics]
    ,p.last_updated, p.rows, p.rows_sampled, p.modification_counter as [Mod Count]
from
    sys.stats s join sys.tables t on
        s.object_id = t.object_id
    join sys.schemas sc on
        t.schema_id = sc.schema_id
    outer apply
        sys.dm_db_stats_properties(t.object_id,s.stats_id) p
where
    sc.name = 'dbo' and t.name = 'Books';