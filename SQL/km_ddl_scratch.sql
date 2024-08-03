--CREATE OR REPLACE TABLE encounters as
--CREATE TABLE encounters (Id VARCHAR PRIMARY KEY, START TIMESTAMP) as
--CREATE OR REPLACE TABLE patients as
--CREATE OR REPLACE TABLE procedures as
--CREATE OR REPLACE TABLE medications as
CREATE OR REPLACE TABLE allergies as
select 
	*
	--rsv.Id
	--, rsv.START
--from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/encounters.csv', SAMPLE_SIZE = -1) rsv
--from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/patients.csv', SAMPLE_SIZE = -1) rsv
--from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/procedures.csv', SAMPLE_SIZE = -1) rsv
--from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/medications.csv', SAMPLE_SIZE = -1) rsv
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/allergies.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------

--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE patients (
	Id VARCHAR PRIMARY KEY,
	BIRTHDATE DATE,
	DEATHDATE VARCHAR,
	SSN VARCHAR,
	DRIVERS VARCHAR,
	PASSPORT VARCHAR,
	PREFIX VARCHAR,
	"FIRST" VARCHAR,
	"LAST" VARCHAR,
	SUFFIX VARCHAR,
	MAIDEN VARCHAR,
	MARITAL VARCHAR,
	RACE VARCHAR,
	ETHNICITY VARCHAR,
	GENDER VARCHAR,
	BIRTHPLACE VARCHAR,
	ADDRESS VARCHAR,
	CITY VARCHAR,
	STATE VARCHAR,
	ZIP VARCHAR
)
;

------------------------------------------------------------------------------------
--populates new table
insert into patients
	select 
	*
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/patients.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------
--test query to verify results
select 
	tcsv.*
from patients tcsv
limit 100
;
------------------------------------------------------------------------------------
--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE encounters (
	Id VARCHAR PRIMARY KEY
	, "START" TIMESTAMP
	, STOP TIMESTAMP
	, PATIENT VARCHAR
	, PROVIDER VARCHAR
	, ENCOUNTERCLASS VARCHAR
	, CODE VARCHAR
	, DESCRIPTION VARCHAR
	, COST DOUBLE
	, REASONCODE VARCHAR
	, REASONDESCRIPTION VARCHAR
	, FOREIGN KEY (PATIENT) REFERENCES patients (Id)
)
;

------------------------------------------------------------------------------------
--populates new table
insert into encounters
	select 
	*
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/encounters.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------
--test query to verify results
select 
	tcsv.*
from encounters tcsv
limit 100
;
------------------------------------------------------------------------------------
--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE procedures (
	"DATE" DATE
	, "PATIENT.x" VARCHAR
	, ENCOUNTER VARCHAR
	, "CODE.x" VARCHAR
	, "DESCRIPTION.x" VARCHAR
	, "COST.x" DOUBLE
	, "REASONCODE.x" VARCHAR
	, "REASONDESCRIPTION.x" VARCHAR
	, FOREIGN KEY ("PATIENT.x") REFERENCES patients (Id)
	, FOREIGN KEY (ENCOUNTER) REFERENCES encounters (Id)
)
;

------------------------------------------------------------------------------------
--populates new table
insert into procedures
	select 
	*
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/procedures.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------
--test query to verify results
select 
	tcsv.*
from procedures tcsv
limit 100
;

------------------------------------------------------------------------------------
--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE medications (
	"START" DATE
	, STOP VARCHAR
	, PATIENT VARCHAR
	, ENCOUNTER VARCHAR
	, CODE BIGINT
	, DESCRIPTION VARCHAR
	, COST DOUBLE
	, DISPENSES BIGINT
	, TOTALCOST DOUBLE
	, REASONCODE VARCHAR
	, REASONDESCRIPTION VARCHAR
	, FOREIGN KEY (PATIENT) REFERENCES patients (Id)
	--, FOREIGN KEY (ENCOUNTER) REFERENCES encounters (Id) --SQL Error: Constraint Error: Violates foreign key constraint because key "Id: db37351e-7343-4430-ba3e-5d0a0f8bb842" does not exist in the referenced table
)
;

------------------------------------------------------------------------------------
--populates new table
insert into medications
	select 
	*
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/medications.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------
--test query to verify results
select 
	tcsv.*
from medications tcsv
limit 100
;

------------------------------------------------------------------------------------
--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE allergies (
	"START" DATE
	, STOP VARCHAR
	, PATIENT VARCHAR
	, ENCOUNTER VARCHAR
	, CODE BIGINT
	, DESCRIPTION VARCHAR
	, FOREIGN KEY (PATIENT) REFERENCES patients (Id)
	, FOREIGN KEY (ENCOUNTER) REFERENCES encounters (Id)
)
;

------------------------------------------------------------------------------------
--populates new table
insert into allergies
	select 
	*
from read_csv_auto('C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/allergies.csv', SAMPLE_SIZE = -1) rsv
;


------------------------------------------------------------------------------------
--test query to verify results
select 
	tcsv.*
from allergies tcsv
limit 100
;