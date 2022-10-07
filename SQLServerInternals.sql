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

/*
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

*/


/*
create table dbo.CETest
(    
  	ID int not null,
    ADate date not null,
    Placeholder char(10)
);

;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,IDs(ID) as (select row_number() over (order by (select null)) from N5)

insert into dbo.CETest(ID,ADate)
    select ID,dateadd(day,abs(checksum(newid())) % 365,'2016-06-01') from IDs;

create unique clustered index IDX_CETest_ID on dbo.CETest(ID);
create nonclustered index IDX_CETest_ADate on dbo.CETest(ADate);


DBCC SHOW_STATISTICS('dbo.CETest', IDX_CETest_ADate)

*/


/*


alter database SQLServerInternals set compatibility_level = 120 --数据库兼容级别(110:SQL Server 2012,120;SQL Server 2014,130:SQL Server 2016,140:SQL Server 2017,150:SQL Server 2019)
GO

--查询条件在直方图Key中的值
select ID, ADate, Placeholder
from dbo.CETest with (index=IDX_CETest_ADate)
where ADate = '2016-06-07';



DBCC SHOW_STATISTICS('dbo.CETest', IDX_CETest_ADate)

--查询条件不在直方图Key中的值
select ID, ADate, Placeholder
from dbo.CETest with (index=IDX_CETest_ADate)
where ADate = '2016-06-12';


declare @D date = '2016-06-08';
select ID, ADate, Placeholder
from dbo.CETest with (index=IDX_CETest_ADate)
where ADate = @D;

*/


/*
create table dbo.PageSplitDemo
(    
    ID int not null,
    Data varchar(8000) null
);
create unique clustered index IDX_PageSplitDemo_ID
on dbo.PageSplitDemo(ID);

;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N2 as T2) -- 1,024 rows
,IDs(ID) as (select row_number() over (order by (select NULL)) from N5)

insert into dbo.PageSplitDemo(ID)
    select ID * 2 from Ids where ID <= 620

select page_count, avg_page_space_used_in_percent
from sys.dm_db_index_physical_stats(db_id(),object_id(N'dbo.PageSplitDemo'),1,null
    ,'DETAILED');

*/

/*

alter database SQLServerInternals set compatibility_level = 100 --数据库兼容级别(100:SQL Server 2008,110:SQL Server 2012,120;SQL Server 2014,130:SQL Server 2016,140:SQL Server 2017,150:SQL Server 2019)
GO
--DELETE FROM PageSplitDemo WHERE ID=101

--多页拆分：在表中插入一条大行
insert into dbo.PageSplitDemo(ID,Data) values(1,replicate('a',8000));

select page_count, avg_page_space_used_in_percent
from sys.dm_db_index_physical_stats(db_id(),object_id(N'dbo.PageSplitDemo'),1,null
    ,'DETAILED');
*/


/*
create table dbo.Positions
(    DeviceId int not null,
    ATime datetime2(0) not null,
    Latitude decimal(9,6) not null,
    Longitude decimal(9,6) not null,
    Address nvarchar(200) null,
    Placeholder char(100) null,
);

;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,IDs(ID) as (select row_number() over (order by (select NULL)) from N5)

insert into dbo.Positions(DeviceId, ATime, Latitude, Longitude)
    select
        ID % 100 /*DeviceId*/
        ,dateadd(minute, -(ID % 657), getutcdate()) /*ATime*/
        ,0 /*Latitude - just dummy value*/
        ,0 /*Longitude - just dummy value*/
    from IDs;

create unique clustered index IDX_Postitions_DeviceId_ATime
on dbo.Positions(DeviceId, ATime);

select index_level, page_count, avg_page_space_used_in_percent, avg_fragmentation_in_percent
from sys.dm_db_index_physical_stats(DB_ID(),OBJECT_ID(N'dbo.Positions'),1,null,'DETAILED')

*/

/*

update dbo.Positions set Address = N'Position address';

select index_level, page_count, avg_page_space_used_in_percent, avg_fragmentation_in_percent
from sys.dm_db_index_physical_stats(DB_ID(),OBJECT_ID(N'dbo.Positions'),1,null,'DETAILED')

*/

/*
select avg(datalength(Address)) as [Avg Address Size] from dbo.Positions

*/


/*
truncate table dbo.Positions
go

;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,IDs(ID) as (select row_number() over (order by (select NULL)) from N5)

insert into dbo.Positions(DeviceId, ATime, Latitude, Longitude, Address)
    select
        ID % 100 /*DeviceId*/
        ,dateadd(minute, -(ID % 657), getutcdate()) /*ATime*/
        ,0 /*Latitude - just dummy value*/
        ,0 /*Longitude - just dummy value*/
        ,replicate(N' ',16) /*Address - adding string of 16 space characters*/
    from IDs;

--create unique clustered index IDX_Postitions_DeviceId_ATime ON dbo.Positions(DeviceId, ATime);

update dbo.Positions set Address = N'Position address';

select index_level, page_count, avg_page_space_used_in_percent, avg_fragmentation_in_percent
from sys.dm_db_index_physical_stats(DB_ID(),OBJECT_ID(N'Positions'),1,null,'DETAILED')

*/


/*
drop table dbo.Positions
GO

create table dbo.Positions
(    
	DeviceId int not null,
	ATime datetime2(0) not null,
    Latitude decimal(9,6) not null,
    Longitude decimal(9,6) not null,
    Address nvarchar(200) null,
    Placeholder char(100) null,
	Dummy varbinary(32)
);

;with N1(C) as (select 0 union all select 0) -- 2 rows
,N2(C) as (select 0 from N1 as T1 cross join N1 as T2) -- 4 rows
,N3(C) as (select 0 from N2 as T1 cross join N2 as T2) -- 16 rows
,N4(C) as (select 0 from N3 as T1 cross join N3 as T2) -- 256 rows
,N5(C) as (select 0 from N4 as T1 cross join N4 as T2) -- 65,536 rows
,IDs(ID) as (select row_number() over (order by (select NULL)) from N5)
insert into dbo.Positions(DeviceId, ATime, Latitude, Longitude, Dummy)
    SELECT ID % 100 /*DeviceId*/
        ,dateadd(minute, -(ID % 657), getutcdate()) /*ATime*/
        ,0 /*Latitude - just dummy value*/
        ,0 /*Longitude - just dummy value*/
        ,convert(varbinary(32),replicate('0',32)) /* Reserving the space*/
    from IDs;

create unique clustered index IDX_Postitions_DeviceId_ATime
on dbo.Positions(DeviceId, ATime);

update dbo.Positions SET Address = N'Position address',Dummy = null;

select index_level, page_count, avg_page_space_used_in_percent, avg_fragmentation_in_percent
from sys.dm_db_index_physical_stats(DB_ID(),OBJECT_ID(N'Positions'),1,null,'DETAILED')

*/

/*
select database_id as [DB ID], db_name(database_id) as [DB Name]
    ,convert(decimal(11,3),count(*) * 8 / 1024.0) as [Buffer Pool Size (MB)]
from sys.dm_os_buffer_descriptors with (nolock)
group by database_id
order by [Buffer Pool Size (MB)] desc;

*/


/*
;with Waits
as
(    select
            wait_type, wait_time_ms, waiting_tasks_count,signal_wait_time_ms
            ,wait_time_ms - signal_wait_time_ms as resource_wait_time_ms
            ,100. * wait_time_ms / SUM(wait_time_ms) over() as Pct
            ,row_number() over(order by wait_time_ms desc) AS RowNum
    from sys.dm_os_wait_stats with (nolock)
    where
        wait_time_ms > 0 and
            wait_type not in /* Filtering out non-essential system waits */
    (N'CLR_SEMAPHORE',N'LAZYWRITER_SLEEP',N'RESOURCE_QUEUE', N'DBMIRROR_DBM_EVENT'
    ,N'SLEEP_TASK',N'SLEEP_SYSTEMTASK',N'SQLTRACE_BUFFER_FLUSH',N'FSAGENT'
    ,N'DBMIRROR_EVENTS_QUEUE', N'DBMIRRORING_CMD', N'DBMIRROR_WORKER_QUEUE'
    ,N'WAITFOR',N'LOGMGR_QUEUE',N'CHECKPOINT_QUEUE',N'FT_IFTSHC_MUTEX'
    ,N'REQUEST_FOR_DEADLOCK_SEARCH',N'HADR_CLUSAPI_CALL',N'XE_TIMER_EVENT'
    ,N'BROKER_TO_FLUSH',N'BROKER_TASK_STOP',N'CLR_MANUAL_EVENT',N'HADR_TIMER_TASK'
    ,N'CLR_AUTO_EVENT',N'DISPATCHER_QUEUE_SEMAPHORE',N'HADR_LOGCAPTURE_WAIT'
    ,N'FT_IFTS_SCHEDULER_IDLE_WAIT',N'XE_DISPATCHER_WAIT',N'XE_DISPATCHER_JOIN'
    ,N'HADR_NOTIFICATION_DEQUEUE',N'SQLTRACE_INCREMENTAL_FLUSH_SLEEP',N'MSQL_XP'
    ,N'HADR_WORK_QUEUE',N'ONDEMAND_TASK_QUEUE',N'BROKER_EVENTHANDLER'
    ,N'SLEEP_BPOOL_FLUSH',N'KSOURCE_WAKEUP',N'SLEEP_DBSTARTUP',N'DIRTY_PAGE_POLL'
    ,N'BROKER_RECEIVE_WAITFOR',N'MEMORY_ALLOCATION_EXT',N'SNI_HTTP_ACCEPT'
    ,N'PREEMPTIVE_OS_LIBRARYOPS',N'PREEMPTIVE_OS_COMOPS',N'WAIT_XTP_HOST_WAIT'
    ,N'PREEMPTIVE_OS_CRYPTOPS',N'PREEMPTIVE_OS_PIPEOPS',N'WAIT_XTP_CKPT_CLOSE'
    ,N'PREEMPTIVE_OS_AUTHENTICATIONOPS',N'PREEMPTIVE_OS_GENERICOPS',N'CHKPT'
    ,N'PREEMPTIVE_OS_VERIFYTRUST',N'PREEMPTIVE_OS_FILEOPS',N'QDS_ASYNC_QUEUE'
    ,N'PREEMPTIVE_OS_DEVICEOPS',N'HADR_FILESTREAM_IOMGR_IOCOMPLETION'
    ,N'PREEMPTIVE_XE_GETTARGETSTATE',N'SP_SERVER_DIAGNOSTICS_SLEEP'
    ,N'BROKER_TRANSMITTER',N'PWAIT_ALL_COMPONENTS_INITIALIZED'
    ,N'QDS_PERSIST_TASK_MAIN_LOOP_SLEEP',N'PWAIT_DIRECTLOGCONSUMER_GETNEXT'
    ,N'QDS_CLEANUP_STALE_QUERIES_TASK_MAIN_LOOP_SLEEP',N'SERVER_IDLE_CHECK'
    ,N'SLEEP_DCOMSTARTUP',N'SQLTRACE_WAIT_ENTRIES',N'SLEEP_MASTERDBREADY' 
	,N'SLEEP_MASTERMDREADY',N'SLEEP_TEMPDBSTARTUP',N'XE_LIVE_TARGET_TVF'
    ,N'WAIT_FOR_RESULTS',N'WAITFOR_TASKSHUTDOWN',N'PARALLEL_REDO_WORKER_SYNC'
    ,N'PARALLEL_REDO_WORKER_WAIT_WORK',N'SLEEP_MASTERUPGRADED'
    ,N'SLEEP_MSDBSTARTUP',N'WAIT_XTP_OFFLINE_CKPT_NEW_LOG')
)
SELECT w1.wait_type as [Wait Type]
    ,w1.waiting_tasks_count as [Wait Count]
	,convert(decimal(12,3), w1.wait_time_ms / 1000.0) as [Wait Time]
    ,convert(decimal(12,1), w1.wait_time_ms / w1.waiting_tasks_count) AS [Avg Wait Time]
    ,convert(decimal(12,3), w1.signal_wait_time_ms / 1000.0)
	as [Signal Wait Time]
    ,convert(decimal(12,1), w1.signal_wait_time_ms / w1.waiting_tasks_count)
        as [Avg Signal Wait Time]
    ,convert(decimal(12,3), w1.resource_wait_time_ms / 1000.0)
        as [Resource Wait Time]
    ,convert(decimal(12,1), w1.resource_wait_time_ms / w1.waiting_tasks_count)
        as [Avg Resource Wait Time]
    ,convert(decimal(6,3), w1.Pct) as [Percent]
    ,convert(decimal(6,3), w1.Pct + IsNull(w2.Pct,0)) as [Running Percent]
from
    Waits w1 cross apply
    (
            select sum(w2.Pct) as Pct
            from Waits w2
            where w2.RowNum < w1.RowNum
    ) w2
where
    w1.RowNum = 1 or w2.Pct <= 99
order by
    w1.RowNum
option (recompile);

*/




/*
-- 显示了一个获取服务器上所有数据库的I/O统计信息的查询

select
    fs.database_id as [DB ID], fs.file_id as [File Id], mf.name as [File Name]
    ,mf.physical_name as [File Path], mf.type_desc as [Type], fs.sample_ms as [Time]
    ,fs.num_of_reads as [Reads], fs.num_of_bytes_read as [Read Bytes]
    ,fs.num_of_writes as [Writes], fs.num_of_bytes_written as [Written Bytes]
	 ,fs.num_of_reads + fs.num_of_writes as [IO Count]
    ,convert(decimal(5,2),100.0 * fs.num_of_bytes_read /
        (fs.num_of_bytes_read + fs.num_of_bytes_written)) as [Read %]
    ,convert(decimal(5,2),100.0 * fs.num_of_bytes_written /
        (fs.num_of_bytes_read + fs.num_of_bytes_written)) as [Write %]
    ,fs.io_stall_read_ms as [Read Stall], fs.io_stall_write_ms as [Write Stall]
    ,case when fs.num_of_reads = 0
        then 0.000
        else convert(decimal(12,3),1.0 * fs.io_stall_read_ms / fs.num_of_reads)
    end as [Avg Read Stall]
    ,case when fs.num_of_writes = 0
        then 0.000
        else convert(decimal(12,3),1.0 * fs.io_stall_write_ms / fs.num_of_writes)
    end as [Avg Write Stall]
from
    sys.dm_io_virtual_file_stats(null,null) fs join
        sys.master_files mf with (nolock) on
            fs.database_id = mf.database_id and fs.file_id = mf.file_id
        join sys.databases d with (nolock) on
            d.database_id = fs.database_id
where
    fs.num_of_reads + fs.num_of_writes > 0;


	*/



/*
-- 显示前50个I/O最密集的查询
	select top 50
	    substring(qt.text, (qs.statement_start_offset/2)+1,
    ((
        case qs.statement_end_offset
            when -1 then datalength(qt.text)
            else qs.statement_end_offset
        end - qs.statement_start_offset)/2)+1) as SQL
    ,qp.query_plan as [Query Plan]
    ,qs.execution_count as [Exec Cnt]
    ,(qs.total_logical_reads + qs.total_logical_writes) / qs.execution_count as [Avg IO]
    ,qs.total_logical_reads as [Total Reads], qs.last_logical_reads as [Last Reads]
    ,qs.total_logical_writes as [Total Writes], qs.last_logical_writes as [Last Writes]
    ,qs.total_worker_time as [Total Worker Time], qs.last_worker_time as [Last Worker Time]
    ,qs.total_elapsed_time / 1000 as [Total Elapsed Time]
    ,qs.last_elapsed_time / 1000 as [Last Elapsed Time]
    ,qs.last_execution_time as [Last Exec Time]
    ,qs.total_rows as [Total Rows], qs.last_rows as [Last Rows]
    ,qs.min_rows as [Min Rows], qs.max_rows as [Max Rows]
from
    sys.dm_exec_query_stats qs with (nolock)
        cross apply sys.dm_exec_sql_text(qs.sql_handle) qt
        cross apply sys.dm_exec_query_plan(qs.plan_handle) qp
order by
     [Avg IO] desc

*/

/*
-- 显示前50个I/O最密集的存储过程
select top 50
    db_name(ps.database_id) as [DB]
    ,object_name(ps.object_id, ps.database_id) as [Proc Name]
    ,ps.type_desc as [Type]
    ,qp.query_plan as  [Plan]
    ,ps.execution_count as [Exec Count]
    ,(ps.total_logical_reads + ps.total_logical_writes) / ps.execution_count as [Avg IO]
    ,ps.total_logical_reads as [Total Reads], ps.last_logical_reads as [Last Reads]
    ,ps.total_logical_writes as [Total Writes], ps.last_logical_writes as [Last Writes]
    ,ps.total_worker_time as [Total Worker Time], ps.last_worker_time as [Last Worker Time]
    ,ps.total_elapsed_time / 1000 as [Total Elapsed Time]
    ,ps.last_elapsed_time / 1000 as [Last Elapsed Time]
    ,ps.last_execution_time as [Last Exec Time]
from
    sys.dm_exec_procedure_stats ps with (nolock)
        cross apply sys.dm_exec_query_plan(ps.plan_handle) qp
order by
     [Avg IO] desc

	 */


/*
select
mg.session_id, t.text as [SQL], qp.query_plan as [Plan], mg.is_small, mg.dop
,mg.query_cost, mg.request_time, mg.required_memory_kb, mg.requested_memory_kb
,mg.wait_time_ms, mg.grant_time, mg.granted_memory_kb, mg.used_memory_kb
,mg.max_used_memory_kb
from
sys.dm_exec_query_memory_grants mg with (nolock)
cross apply sys.dm_exec_sql_text(mg.sql_handle) t
cross apply sys.dm_exec_query_plan(mg.plan_handle) as qp
*/

/*
-- 从缓存的计划中获取内存授予信息
;with xmlnamespaces(default 'http://schemas.microsoft.com/sqlserver/2004/07/showplan')
,Statements(PlanHandle, ObjType, UseCount, StmtSimple)
as
(
select cp.plan_handle, cp.objtype, cp.usecounts, nodes.stmt.query('.')
from sys.dm_exec_cached_plans cp with (nolock)
cross apply sys.dm_exec_query_plan(cp.plan_handle) qp
cross apply qp.query_plan.nodes('//StmtSimple') nodes(stmt)
)
select top 50
s.PlanHandle, s.ObjType, s.UseCount
,p.qp.value('@CachedPlanSize','int') as CachedPlanSize
,mg.mg.value('@SerialRequiredMemory','int') as [SerialRequiredMemory KB]
,mg.mg.value('@SerialDesiredMemory','int') as [SerialDesiredMemory KB]
from Statements s
cross apply s.StmtSimple.nodes('.//QueryPlan') p(qp)
cross apply p.qp.nodes('.//MemoryGrantInfo') mg(mg)
order by
mg.mg.value('@SerialRequiredMemory','int') desc

*/

/*
select type, pages_in_bytes
    ,case
        when (creation_options & 0x20 = 0x20)
            then 'Global PMO. Cannot be partitioned by CPU/NUMA Node. T8048 not applicable.'
        when (creation_options & 0x40 = 0x40)
            then 'Partitioned by CPU. T8048 not applicable.'
        when (creation_options & 0x80 = 0x80)
            then 'Partitioned by Node. Use T8048 to further partition by CPU.'
        else 'Unknown'
    end as [Partitioning Type]
from sys.dm_os_memory_objects
order by pages_in_bytes desc

*/


/*
select
    sum(signal_wait_time_ms) as [Signal Wait Time (ms)]
    ,convert(decimal(7,4), 100.0 * sum(signal_wait_time_ms) /
        sum (wait_time_ms)) as [% Signal waits]
    ,sum(wait_time_ms - signal_wait_time_ms) as [Resource Wait Time (ms)]
    ,convert(decimal(7,4), 100.0 * sum(wait_time_ms - signal_wait_time_ms) /
        sum (wait_time_ms)) as [% Resource waits]
from
    sys.dm_os_wait_stats with (nolock)

*/

;WITH Latches
AS (SELECT latch_class,
           wait_time_ms,
           waiting_requests_count,
           100. * wait_time_ms / SUM(wait_time_ms) OVER () AS Pct,
           ROW_NUMBER() OVER (ORDER BY wait_time_ms DESC) AS RowNum
    FROM sys.dm_os_latch_stats WITH (NOLOCK)
    WHERE latch_class NOT IN ( N'BUFFER', N'SLEEP_TASK' )
          AND wait_time_ms > 0)
SELECT l1.latch_class AS [Latch Type],
       l1.waiting_requests_count AS [Wait Count],
       CONVERT(DECIMAL(12, 3), l1.wait_time_ms / 1000.0) AS [Wait Time],
       CONVERT(DECIMAL(12, 1), l1.wait_time_ms / l1.waiting_requests_count) AS [Avg Wait Time],
       CONVERT(DECIMAL(6, 3), l1.Pct) AS [Percent],
       CONVERT(DECIMAL(6, 3), l1.Pct + ISNULL(l2.Pct, 0)) AS [Running Percent]
FROM Latches l1
    CROSS APPLY
(
    SELECT SUM(l2.Pct) AS Pct
    FROM Latches l2
    WHERE l2.RowNum < l1.RowNum
) l2
WHERE l1.RowNum = 1
      OR l2.Pct < 99
OPTION (RECOMPILE);