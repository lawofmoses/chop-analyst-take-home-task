------------------------------------------------------------------------------------
--Developer: Kellon Moses

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


-
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
--creates new table (to identify keys/relationships)
CREATE OR REPLACE TABLE medications (
	"START" DATE
	, STOP VARCHAR
	--, STOP DATE
	, PATIENT VARCHAR
	, ENCOUNTER VARCHAR
	--, CODE BIGINT
	, CODE VARCHAR
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
--exports final output to CSV
copy
(
--final output
select
	pat.Id as PATIENT_ID
	, enc.Id as ENCOUNTER_ID
	, enc.START as HOSPITAL_ENCOUNTER_DATE
	, datediff('year', pat.BIRTHDATE,enc.START) as AGE_AT_VISIT
	--, pat.DEATHDATE
	, case
		when pat.DEATHDATE Not In ('NA') then 1
		else 0
		end as DEATH_AT_VISIT_IND
	, act_med.COUNT_CURRENT_MEDS
	, case
		when med.CODE In('316049','429503','406022') then 1 --Hydromorphone 325Mg, Fentanyl – 100 MCG, Oxycodone-acetaminophen 100 Ml
		else 0
		end as CURRENT_OPIOID_IND
	, enc.READMISSION_90_DAY_IND
	, enc.READMISSION_30_DAY_IND
	, min(enc.FIRST_READMISSION_DATE) over (partition by enc.PATIENT, enc.REASONCODE order by case when enc.FIRST_READMISSION_DATE is not null then enc.START end) as FIRST_READMISSION_DATE
from patients pat
--left join encounters enc on enc.PATIENT = pat.Id
left join
(
select
	enc.*
	, lag(enc.START) over (partition by enc.PATIENT, enc.REASONCODE order by enc.START desc) as StartLag
	, DENSE_RANK() over (partition by enc.PATIENT, enc.REASONCODE order by case when enc.REASONCODE In('55680006') then enc.START end) as ReadmitRank
	, case
		when enc.REASONCODE In('55680006') and datediff('day', enc.START, lag(enc.START) over (partition by enc.PATIENT, enc.REASONCODE order by enc.START desc)) <= 90 then lag(enc.START) over (partition by enc.PATIENT, enc.REASONCODE order by enc.START desc)
		end as FIRST_READMISSION_DATE
	, case
		when datediff('day', enc.START, lag(enc.START) over (partition by enc.PATIENT, enc.REASONCODE order by enc.START desc)) <= 90 then 1
		else 0
		end as READMISSION_90_DAY_IND
	, case
		when datediff('day', enc.START, lag(enc.START) over (partition by enc.PATIENT, enc.REASONCODE order by enc.START desc)) <= 30 then 1
		else 0
		end as READMISSION_30_DAY_IND
from encounters enc
) enc on enc.PATIENT = pat.Id
left join medications med on med.PATIENT = pat.Id
and med.ENCOUNTER = enc.Id
left join --active meds
(
select
	act_med_sub.PATIENT
	, count(distinct act_med_sub.CODE) as COUNT_CURRENT_MEDS
from medications act_med_sub
where
	act_med_sub.STOP is not null
group by
	act_med_sub.PATIENT
) act_med on act_med.PATIENT = pat.Id
where
	enc.REASONCODE In('55680006') --overdoses
	and enc.START >= '1999-07-15'
	and 
	(
		datediff('year', pat.BIRTHDATE,enc.START) between 18 and 35
	)
)
to 'C:/Users/kello/Documents/Code/analyst-take-home-task-master/datasets/Kellon_Moses.csv' (HEADER, DELIMITER ',')
;