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
	enc_sub.*
	, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc) as StartLag
	, DENSE_RANK() over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by case when enc_sub.REASONCODE In('55680006') then enc_sub.START end) as ReadmitRank
	, case
		when enc_sub.REASONCODE In('55680006') and datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 90 then lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)
		end as FIRST_READMISSION_DATE
	, case
		when datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 90 then 1
		else 0
		end as READMISSION_90_DAY_IND
	, case
		when datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 30 then 1
		else 0
		end as READMISSION_30_DAY_IND
from encounters enc_sub
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
;
---------------------------------------------------------------------------
--testing query
select
	pat.*
	, enc.START
	, enc.STOP
	, enc.REASONCODE
	, enc.REASONDESCRIPTION
	, med.CODE
	, med.DESCRIPTION
	, pat.DEATHDATE
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
	enc_sub.*
	, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc) as StartLag
	, DENSE_RANK() over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by case when enc_sub.REASONCODE In('55680006') then enc_sub.START end) as ReadmitRank
	, case
		when enc_sub.REASONCODE In('55680006') and datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 90 then lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)
		end as FIRST_READMISSION_DATE
	, case
		when datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 90 then 1
		else 0
		end as READMISSION_90_DAY_IND
	, case
		when datediff('day', enc_sub.START, lag(enc_sub.START) over (partition by enc_sub.PATIENT, enc_sub.REASONCODE order by enc_sub.START desc)) <= 30 then 1
		else 0
		end as READMISSION_30_DAY_IND
from encounters enc_sub
where
	enc_sub.REASONCODE In('55680006')
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
	/* and 
	(
		datediff('year', pat.BIRTHDATE,enc.START) between 18 and 35
	) */
	--and enc.PATIENT In('1311c221-821a-4e04-9718-07dc5b6f563f') --for testing only
	and enc.PATIENT In('1311c221-821a-4e04-9718-07dc5b6f563f') --for testing only
;
---------------------------------------------------------------------------
--testing query
select
	act_med_sub.PATIENT
	, count(distinct act_med_sub.CODE) as COUNT_CURRENT_MEDS
from medications act_med_sub
where
	act_med_sub.STOP is not null
	and act_med_sub.PATIENT In('67fe56c2-2ecc-4744-b844-1221768f457f')
group by
	act_med_sub.PATIENT
;

---------------------------------------------------------------------------
--testing query
select
	act_med_sub.*
from medications act_med_sub
where
	act_med_sub.STOP is not null
	and act_med_sub.PATIENT In('67fe56c2-2ecc-4744-b844-1221768f457f')
;

---------------------------------------------------------------------------
--testing query
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
where
	--enc.PATIENT In('1311c221-821a-4e04-9718-07dc5b6f563f')
	enc.PATIENT In('58240155-9e6a-486f-ae16-3d82c037f758')
	and enc.REASONCODE In('55680006')
;