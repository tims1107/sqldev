create or replace PROCEDURE  SP_ASR_PROC_TRACK_RESULTS (
	p_state IN varchar2,
  	p_gtt_count out number
) AS
	v_process  VARCHAR2(30) := 'ASR_PROCESS_' || UPPER(p_state);
	v_next_time TIMESTAMP(6) := SYSTIMESTAMP;
	v_start_time TIMESTAMP(6);
	v_error_flag BOOLEAN := FALSE;
	v_count NUMBER := 0;
  v_sql VARCHAR2(4000);
  v_gtt_count number := 0;

  v_order_number varchar2(25) := '';
  v_gtt_filter_count number := 0;
	
  v_noaddr_order_number varchar2(25) := null;
  v_noaddr_accession_number varchar2(25) := null;
  v_noaddr_otc varchar2(25) := null;
  v_noaddr_rtc varchar2(25) := null;  
  
    table_or_view_not_exist exception;
    pragma exception_init(table_or_view_not_exist, -942);
    attempted_ddl_on_in_use_GTT exception;
    pragma exception_init(attempted_ddl_on_in_use_GTT, -14452);
    
	BEGIN
     BEGIN
/*     
         EXECUTE IMMEDIATE 'truncate table STATERPT_OWNER.GTT_RESULTS_EXTRACT';
         COMMIT;
         
         EXECUTE IMMEDIATE 'truncate table STATERPT_OWNER.GTT_STAFF_RESULTS_EXTRACT';
         COMMIT;         
         
         EXECUTE IMMEDIATE 'truncate table STATERPT_OWNER.GTT_PM';
         COMMIT; 
*/
        delete from STATERPT_OWNER.GTT_RESULTS_EXTRACT where accession_number is not null;
        commit;
        delete from STATERPT_OWNER.GTT_STAFF_RESULTS_EXTRACT where accession_number is not null;
        commit;
      END;  
  
		BEGIN

    --- gtt_pm
/*    
        insert into staterpt_owner.gtt_pm
        select
          EID,
          MRN,
          LNAME,
          FNAME,
          MNAME,
          STLINE1,
          STLINE2,
          CITY,
          STATE,
          ZIPCODE,
          --PHNUMBER,
          (
             CASE
               WHEN PHNUMBER is null THEN null
               WHEN instr(PHNUMBER, '-') > 0	THEN replace(replace(replace(PHNUMBER, '-'), '('), ')')
               ELSE PHNUMBER
             END
          ) PHNUMBER,          
          DOB,
          SSN,
          SEX,
          TABLE_UPDATED,
            sysdate,
          lab_fk  
        from
          staterpt_owner.patientmaster
        where 
          --(state = p_state or state is null);
          ((state = p_state) or (state is null) or (upper(state) = upper('null')));
          
        commit;
        
        update
          staterpt_owner.gtt_pm
        set
          state = null
        where
          (upper(state) = upper('null'));        
        
        delete from staterpt_owner.gtt_pm where rowid in
        ( 
          select "rowid" from
               (select "rowid", rank_n from
                   (select rank() over (partition by eid order by rowid) rank_n, rowid as "rowid"
                       from staterpt_owner.gtt_pm
                       where eid in
                          (
                            select eid from staterpt_owner.gtt_pm
                              where (state = p_state or state is null)
                              group by eid
                              having count(*) > 1
                          )
                       )
                   )
               where rank_n > 1
        );
        
        commit;
*/
      --SP_ASR_NORMALIZE_GTT_PM(p_state);
       --- end gtt_pm  

/*
		INSERT INTO 
			STATERPT_OWNER.ASR_PROCESS_TRACKING
			(start_time, process_name, status, last_updated)
		VALUES 
			(v_next_time, v_process, '0', v_next_time);
		COMMIT;
*/

		SELECT 
			MAX (ASR_PROCESS_TRACKING.start_time)
		INTO   
			v_start_time
		FROM   
			STATERPT_OWNER.ASR_PROCESS_TRACKING
		WHERE  
			ASR_PROCESS_TRACKING.process_name = v_process
			and ASR_PROCESS_TRACKING.status = '1';

		dbms_output.put_line('v_start_time = ' || v_start_time);
		dbms_output.put_line('v_next_time = ' || v_next_time);

/*
		INSERT INTO
			STATERPT_OWNER.GTT_RESULTS_EXTRACT
      
                select
                  distinct(re.ACCESSION_NUMBER) accession_no,
                  f.FACILITY_ID,
                  a.CID,
                  null ethnic_group,
                  dp.RACE patient_race,
                  lo.EXTERNAL_MRN mrn,
                 
                  nvl(p.lname, '') PATIENT_LAST_NAME,
                  nvl(p.fname, '') PATIENT_FIRST_NAME,
                  --p.mname PATIENT_MIDDLE_NAME,
                  (
                    CASE
                      WHEN p.mname is null THEN null
                      WHEN upper(p.mname) = 'NULL' THEN null
                      ELSE p.mname
                    END
                  ) PATIENT_MIDDLE_NAME,          
                  --to_date(p.DOB, 'YYYY-MM-DD') date_of_birth,
                  (
                    CASE
                      WHEN p.DOB is null THEN null
                      WHEN test_date(p.DOB) = 'Valid' THEN
                        (
                          CASE 
                            WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN NULL 
                            ELSE to_date(p.DOB, 'YYYY-MM-DD')
                          END
                        )
                      ELSE NULL	
                    END
                  ) date_of_birth,          
                  p.sex gender,
                  p.ssn patient_ssn,
                  --dph.NPI,
                  --dph.PHYSICIAN_NAME ordering_physician_name,
                  null NPI,
                  lo.ordering_physician_name ordering_physician_name,          
                  lod.REPORT_NOTES,
                  lod.SPECIMEN_RECEIVED_DATE_TIME specimen_receive_date,
                  lod.COLLECTION_DATE collection_date,
                  lod.COLLECTION_TIME collection_time,
                  lod.COLLECTION_DATE_TIME,
                  lod.DRAW_FREQUENCY draw_freq,
                  lod.RESULT_RPT_CHNG_DATE_TIME res_rprt_status_chng_dt_time,
                  lod.ORDER_DETAIL_STATUS,
                  re.ORDER_TEST_CODE,
                  re.ORDER_TEST_NAME,
                  re.RESULT_TEST_CODE,
                  re.RESULT_TEST_NAME,
                  re.RESULT_STATUS,
                  re.TEXTUAL_RESULT,
                  re.TEXTUAL_RESULT_FULL,
                  re.NUMERIC_RESULT,
                  re.UNIT_OF_MEASURE units,
                  re.REFERENCE_RANGE,
                  re.ABNORMAL_FLAG,
                  re.RELEASE_DATE_TIME,
                  trim(dbms_lob.substr( re.RESULT_COMMENT, 4000, 1 )) as RESULT_COMMENTS,
                  re.PERFORMING_LAB performing_lab_id,
                  lod.TEST_CATEGORY order_method,
                  lod.SPECIMEN_METHOD_DESC specimen_source,
                  re.REQUISITION_ID order_number,
                  dl.LAB_ID logging_site,
                  (
                    CASE
                      WHEN p.DOB is null THEN 0
                      WHEN test_date(p.DOB) = 'Valid' THEN
                        (
                          CASE 
                            WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN 0 
                            ELSE trunc(months_between(sysdate, to_date(p.DOB, 'YYYY-MM-DD'))/12)
                          END
                        )
                      ELSE NULL	
                    END
                  ) age,
                  f.DISPLAY_NAME facility_name,
                  null cond_code,
                  lo.PATIENT_TYPE,
                  lod.ORDER_OCCURRENCE_ID source_of_comment,
                  lo.INITIATE_ID	patient_id,
                  lo.ALTERNATE_PATIENT_ID,
                  lo.REQUISITION_STATUS,
                  f.ADDRESS_LINE1 facility_address1,
                  f.ADDRESS_LINE2 facility_address2,
                  f.CITY facility_city,
                  f.STATE facility_state,
                  f.ZIP facility_zip,
                  f.PHONE_NUMBER facility_phone,
                  p.stline1 patient_account_address1,
                  p.stline2 patient_account_address2,
                  p.CITY patient_account_city,
                  p.STATE patient_account_state,
                  p.zipcode patient_account_zip,
                  p.phnumber patient_home_phone,
                  re.LOINC_CODE,
                  re.LOINC_NAME,
                  re.VALUE_TYPE,
                  f.EAST_WEST_FLAG,
                  f.INTERNAL_EXTERNAL_FLAG,
                  re.LAST_UPDATED_DATE last_update_time,
                  re.RESULT_SEQUENCE sequence_no,
                  f.ACCOUNT_STATUS facility_account_status,
                  f.FACILITY_ACTIVE_FLAG,
                  re.MICRO_ISOLATE,
                  re.MICRO_ORGANISM_NAME,
                  re.lab_fk,
                  f.CLINICAL_MANAGER,
                  dl.MEDICAL_DIRECTOR,
                  f.FACILITY_ID acti_facility_id,
                  f.FMC_NUMBER,
                  null reportable_state,
                  null source_state
                from
                  (
                    select
                      r.*
                    from
                      IH_DW.RESULTS r,
                      (
                        select
                          distinct(requisition_id)
                        from
                          IH_DW.DW_ODS_ACTIVITY
                        where
                          --requisition_id in ('650445L')
                          --requisition_id in ('7321PFL')
                          --requisition_id in ('02878ZX')
                          --requisition_id in ('9062FMF')
                          --requisition_id = '9831ZWF'
                          LAST_UPDATED_DATE >= v_start_time
                          
                          --LAST_UPDATED_DATE >= '07-NOV-18 12.00.00.000000000 AM'
                          --and last_updated_date < '07-NOV-18 11.00.00.000000000 PM'
                          --LAST_UPDATED_DATE >= '06-DEC-18 12.00.00.000000000 AM'
                          --and last_updated_date < '06-DEC-18 11.00.00.000000000 PM'                  
                          
                          --and last_updated_date < '25-MAR-18 12.00.00.000000000 AM' 
                          --LAST_UPDATED_DATE >= '01-APR-17 12.00.00.000000000 AM'
                          --and last_updated_date < '05-APR-17 12.00.00.000000000 AM'                  

                      ) a
                    where
                      r.requisition_id = a.requisition_id
                  ) re,
                  IH_DW.DIM_LAB_ORDER lo,
                  IH_DW.DIM_LAB_ORDER_DETAILS lod,
                  --STATERPT_OWNER.PatientMaster p,
                  staterpt_owner.gtt_pm p,
                  IH_DW.DIM_ACCOUNT a,
                  IH_DW.DIM_FACILITY f,
                  --IH_DW.DIM_PHYSICIAN dph,
                  IH_DW.DIM_LAB dl,
                  IH_DW.DIM_PATIENT dp,
                  IH_DW.SPECTRA_MRN_ASSOCIATIONS asso	
                where
                  lo.requisition_id = re.requisition_id
                  and re.LAB_ORDER_FK = lo.LAB_ORDER_PK
                  and lo.initiate_id = p.eid
                  and re.LAB_ORDER_DETAILS_FK = lod.LAB_ORDER_DETAILS_PK
                  and lo.LAB_ORDER_PK = lod.LAB_ORDER_FK
                  and lo.account_fk = a.account_pk	
                  and a.facility_fk = f.facility_pk
                  --and lo.ORDERING_PHYSICIAN_NPI = dph.NPI
                  and re.lab_fk = dl.lab_pk
                  and lo.lab_fk = dl.lab_pk
                  and lo.SPECTRA_MRN_ASSC_FK = asso.SPECTRA_MRN_ASSC_pk
                  and dp.SPECTRA_MRN_FK = asso.SPECTRA_MRN_fK 
                  and dp.FACILITY_FK = asso.FACILITY_fK
                  and lod.TEST_CATEGORY in ('IMMUNO','IMMUN','PCR','ARUP');      
*/
      

      
      
            INSERT INTO
              STATERPT_OWNER.GTT_RESULTS_EXTRACT
                      select
                        distinct(re.ACCESSION_NUMBER) accession_no,
                        f.FACILITY_ID,
                        a.CID,
                        null ethnic_group,
                        dp.RACE patient_race,
                        lo.EXTERNAL_MRN mrn,
                       
                        nvl(p.lname, '') PATIENT_LAST_NAME,
                        nvl(p.fname, '') PATIENT_FIRST_NAME,
                        --p.mname PATIENT_MIDDLE_NAME,
                        (
                          CASE
                            WHEN p.mname is null THEN null
                            WHEN upper(p.mname) = 'NULL' THEN null
                            ELSE p.mname
                          END
                        ) PATIENT_MIDDLE_NAME,          
                        --to_date(p.DOB, 'YYYY-MM-DD') date_of_birth,
                        (
                          CASE
                            WHEN p.DOB is null THEN null
                            WHEN test_date(p.DOB) = 'Valid' THEN
                              (
                                CASE 
                                  WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN NULL 
                                  ELSE to_date(p.DOB, 'YYYY-MM-DD')
                                END
                              )
                            ELSE NULL	
                          END
                        ) date_of_birth,          
                        p.sex gender,
                        p.ssn patient_ssn,
                        --dph.NPI,
                        --dph.PHYSICIAN_NAME ordering_physician_name,
                        null NPI,
                        lo.ordering_physician_name ordering_physician_name,          
                        lod.REPORT_NOTES,
                        lod.SPECIMEN_RECEIVED_DATE_TIME specimen_receive_date,
                        lod.COLLECTION_DATE collection_date,
                        lod.COLLECTION_TIME collection_time,
                        lod.COLLECTION_DATE_TIME,
                        lod.DRAW_FREQUENCY draw_freq,
                        lod.RESULT_RPT_CHNG_DATE_TIME res_rprt_status_chng_dt_time,
                        lod.ORDER_DETAIL_STATUS,
                        re.ORDER_TEST_CODE,
                        re.ORDER_TEST_NAME,
                        re.RESULT_TEST_CODE,
                        re.RESULT_TEST_NAME,
                        re.RESULT_STATUS,
                        re.TEXTUAL_RESULT,
                        re.TEXTUAL_RESULT_FULL,
                        re.NUMERIC_RESULT,
                        re.UNIT_OF_MEASURE units,
                        re.REFERENCE_RANGE,
                        re.ABNORMAL_FLAG,
                        re.RELEASE_DATE_TIME,
                        trim(dbms_lob.substr( re.RESULT_COMMENT, 4000, 1 )) as RESULT_COMMENTS,
                        re.PERFORMING_LAB performing_lab_id,
                        lod.TEST_CATEGORY order_method,
                        lod.SPECIMEN_METHOD_DESC specimen_source,
                        re.REQUISITION_ID order_number,
                        dl.LAB_ID logging_site,
                        (
                          CASE
                            WHEN p.DOB is null THEN 0
                            WHEN test_date(p.DOB) = 'Valid' THEN
                              (
                                CASE 
                                  WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN 0 
                                  ELSE trunc(months_between(sysdate, to_date(p.DOB, 'YYYY-MM-DD'))/12)
                                END
                              )
                            ELSE NULL	
                          END
                        ) age,
                        f.DISPLAY_NAME facility_name,
                        null cond_code,
                        lo.PATIENT_TYPE,
                        lod.ORDER_OCCURRENCE_ID source_of_comment,
                        lo.INITIATE_ID	patient_id,
                        lo.ALTERNATE_PATIENT_ID,
                        lo.REQUISITION_STATUS,
                        f.ADDRESS_LINE1 facility_address1,
                        f.ADDRESS_LINE2 facility_address2,
                        f.CITY facility_city,
                        f.STATE facility_state,
                        f.ZIP facility_zip,
                        f.PHONE_NUMBER facility_phone,
                        p.stline1 patient_account_address1,
                        p.stline2 patient_account_address2,
                        p.CITY patient_account_city,
                        p.STATE patient_account_state,
                        p.zipcode patient_account_zip,
                        --p.phnumber patient_home_phone,
                    (
                       CASE
                         WHEN p.phnumber is null THEN null
                         WHEN length(p.phnumber) > 10 THEN substr(p.phnumber, ((length(p.phnumber) - 10) + 1))
                         ELSE p.phnumber
                       END
                     ) patient_home_phone,                        
                        re.LOINC_CODE,
                        re.LOINC_NAME,
                        re.VALUE_TYPE,
                        f.EAST_WEST_FLAG,
                        f.INTERNAL_EXTERNAL_FLAG,
                        re.LAST_UPDATED_DATE last_update_time,
                        re.RESULT_SEQUENCE sequence_no,
                        f.ACCOUNT_STATUS facility_account_status,
                        f.FACILITY_ACTIVE_FLAG,
                        re.MICRO_ISOLATE,
                        re.MICRO_ORGANISM_NAME,
                        re.lab_fk,
                        f.CLINICAL_MANAGER,
                        dl.MEDICAL_DIRECTOR,
                        f.FACILITY_ID acti_facility_id,
                        f.FMC_NUMBER,
                        null reportable_state,
                        null source_state
                      from
                        (
                          select
                            r.*
                          from
                            IH_DW.RESULTS r,
                            (
                              select
                                distinct(requisition_id)
                              from
                                IH_DW.DW_ODS_ACTIVITY
                              where
                              -- comment
                              --requisition_id ='98848VL'
                                --requisition_id in ('4421JKX','501922X','46432AX','5271ZGX','5532T2X','53962AX') -- OR
                                --requisition_id in ('00506RW')
                                --requisition_id in ('8187HBL')
                                --requisition_id in ('35568UX')
                                --requisition_id in ('81113GL')
                                --requisition_id in ('27632GX','24122VX')
                                --requisition_id in ('650445L')
                                --requisition_id in ('7321PFL')
                                --requisition_id in ('02878ZX')
                                --requisition_id in ('9062FMF')
                                --requisition_id = '9831ZWF'
                                
                                -- uncomment
                                LAST_UPDATED_DATE >= v_start_time
                                
                                --((LAST_UPDATED_DATE >= v_start_time) and (last_updated_date < (SYSTIMESTAMP - INTERVAL '1' HOUR)))
                                
                                --LAST_UPDATED_DATE >= '13-FEB-19 03.00.00.000000000 PM'
                                --and last_updated_date < '14-FEB-19 03.00.00.000000000 AM'                                
                                
                                --LAST_UPDATED_DATE >= '07-NOV-18 12.00.00.000000000 AM'
                                --and last_updated_date < '07-NOV-18 11.00.00.000000000 PM'
                                --LAST_UPDATED_DATE >= '06-DEC-18 12.00.00.000000000 AM'
                                --and last_updated_date < '06-DEC-18 11.00.00.000000000 PM'                  
                                
                                --and last_updated_date < '25-MAR-18 12.00.00.000000000 AM' 
                                --LAST_UPDATED_DATE >= '01-APR-17 12.00.00.000000000 AM'
                                --and last_updated_date < '05-APR-17 12.00.00.000000000 AM'                  
      
                            ) a
                          where
                            r.requisition_id = a.requisition_id
                        ) re,
                        IH_DW.DIM_LAB_ORDER lo,
                        IH_DW.DIM_LAB_ORDER_DETAILS lod,
                        STATERPT_OWNER.PatientMaster p,
                        --staterpt_owner.gtt_pm p,
                        IH_DW.DIM_ACCOUNT a,
                        IH_DW.DIM_FACILITY f,
                        --IH_DW.DIM_PHYSICIAN dph,
                        IH_DW.DIM_LAB dl,
                        IH_DW.DIM_PATIENT dp,
                        IH_DW.SPECTRA_MRN_ASSOCIATIONS asso	
                      where
                        lo.requisition_id = re.requisition_id
                        and re.LAB_ORDER_FK = lo.LAB_ORDER_PK
                        and lo.initiate_id = p.eid
                        and p.lab_fk = re.lab_fk
                        and re.LAB_ORDER_DETAILS_FK = lod.LAB_ORDER_DETAILS_PK
                        and lo.LAB_ORDER_PK = lod.LAB_ORDER_FK
                        and lo.account_fk = a.account_pk	
                        and a.facility_fk = f.facility_pk
                        --and lo.ORDERING_PHYSICIAN_NPI = dph.NPI
                        and re.lab_fk = dl.lab_pk
                        and lo.lab_fk = dl.lab_pk
                        and lo.SPECTRA_MRN_ASSC_FK = asso.SPECTRA_MRN_ASSC_pk
                        and dp.SPECTRA_MRN_FK = asso.SPECTRA_MRN_fK 
                        and dp.FACILITY_FK = asso.FACILITY_fK
                        and lod.TEST_CATEGORY in ('IMMUNO','IMMUN','PCR','ARUP');
                        --and lod.TEST_CATEGORY in ('IMMUNO','IMMUN','PCR','ARUP','HEMA');
                        --and p.state = p_state;
                        --and p.state in (p_state);
                        --and p.state in ('TX','AZ','CA','PA','OR','FL','WA');
                        --and p.state = 'CA';



            INSERT INTO
              STATERPT_OWNER.GTT_STAFF_RESULTS_EXTRACT
                      select
                        distinct(re.ACCESSION_NUMBER) accession_no,
                        f.FACILITY_ID,
                        a.CID,
                        null ethnic_group,
                        --dp.RACE patient_race,
                        null patient_race,
                        lo.EXTERNAL_MRN mrn,
                       
                        nvl(p.lname, '') PATIENT_LAST_NAME,
                        nvl(p.fname, '') PATIENT_FIRST_NAME,
                        --p.mname PATIENT_MIDDLE_NAME,
                        (
                          CASE
                            WHEN p.mname is null THEN null
                            WHEN upper(p.mname) = 'NULL' THEN null
                            ELSE p.mname
                          END
                        ) PATIENT_MIDDLE_NAME,          
                        --to_date(p.DOB, 'YYYY-MM-DD') date_of_birth,
                        (
                          CASE
                            WHEN p.DOB is null THEN null
                            WHEN test_date(p.DOB) = 'Valid' THEN
                              (
                                CASE 
                                  WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN NULL 
                                  ELSE to_date(p.DOB, 'YYYY-MM-DD')
                                END
                              )
                            ELSE NULL	
                          END
                        ) date_of_birth,          
                        p.sex gender,
                        p.ssn patient_ssn,
                        --dph.NPI,
                        --dph.PHYSICIAN_NAME ordering_physician_name,
                        null NPI,
                        lo.ordering_physician_name ordering_physician_name,          
                        lod.REPORT_NOTES,
                        lod.SPECIMEN_RECEIVED_DATE_TIME specimen_receive_date,
                        lod.COLLECTION_DATE collection_date,
                        lod.COLLECTION_TIME collection_time,
                        lod.COLLECTION_DATE_TIME,
                        lod.DRAW_FREQUENCY draw_freq,
                        lod.RESULT_RPT_CHNG_DATE_TIME res_rprt_status_chng_dt_time,
                        lod.ORDER_DETAIL_STATUS,
                        re.ORDER_TEST_CODE,
                        re.ORDER_TEST_NAME,
                        re.RESULT_TEST_CODE,
                        re.RESULT_TEST_NAME,
                        re.RESULT_STATUS,
                        re.TEXTUAL_RESULT,
                        re.TEXTUAL_RESULT_FULL,
                        re.NUMERIC_RESULT,
                        re.UNIT_OF_MEASURE units,
                        re.REFERENCE_RANGE,
                        re.ABNORMAL_FLAG,
                        re.RELEASE_DATE_TIME,
                        trim(dbms_lob.substr( re.RESULT_COMMENT, 4000, 1 )) as RESULT_COMMENTS,
                        re.PERFORMING_LAB performing_lab_id,
                        lod.TEST_CATEGORY order_method,
                        lod.SPECIMEN_METHOD_DESC specimen_source,
                        re.REQUISITION_ID order_number,
                        dl.LAB_ID logging_site,
                        (
                          CASE
                            WHEN p.DOB is null THEN 0
                            WHEN test_date(p.DOB) = 'Valid' THEN
                              (
                                CASE 
                                  WHEN (EXTRACT(YEAR FROM sysdate) - to_number(SUBSTR(replace(p.DOB, '-'), 1, 4))) <= 0 THEN 0 
                                  ELSE trunc(months_between(sysdate, to_date(p.DOB, 'YYYY-MM-DD'))/12)
                                END
                              )
                            ELSE NULL	
                          END
                        ) age,
                        f.DISPLAY_NAME facility_name,
                        null cond_code,
                        lo.PATIENT_TYPE,
                        lod.ORDER_OCCURRENCE_ID source_of_comment,
                        lo.INITIATE_ID	patient_id,
                        lo.ALTERNATE_PATIENT_ID,
                        lo.REQUISITION_STATUS,
                        f.ADDRESS_LINE1 facility_address1,
                        f.ADDRESS_LINE2 facility_address2,
                        f.CITY facility_city,
                        f.STATE facility_state,
                        f.ZIP facility_zip,
                        f.PHONE_NUMBER facility_phone,
                        p.stline1 patient_account_address1,
                        p.stline2 patient_account_address2,
                        p.CITY patient_account_city,
                        p.STATE patient_account_state,
                        p.zipcode patient_account_zip,
                        --p.phnumber patient_home_phone,
                    (
                       CASE
                         WHEN p.phnumber is null THEN null
                         WHEN length(p.phnumber) > 10 THEN substr(p.phnumber, ((length(p.phnumber) - 10) + 1))
                         ELSE p.phnumber
                       END
                     ) patient_home_phone,                        
                        re.LOINC_CODE,
                        re.LOINC_NAME,
                        re.VALUE_TYPE,
                        f.EAST_WEST_FLAG,
                        f.INTERNAL_EXTERNAL_FLAG,
                        re.LAST_UPDATED_DATE last_update_time,
                        re.RESULT_SEQUENCE sequence_no,
                        f.ACCOUNT_STATUS facility_account_status,
                        f.FACILITY_ACTIVE_FLAG,
                        re.MICRO_ISOLATE,
                        re.MICRO_ORGANISM_NAME,
                        re.lab_fk,
                        f.CLINICAL_MANAGER,
                        dl.MEDICAL_DIRECTOR,
                        f.FACILITY_ID acti_facility_id,
                        f.FMC_NUMBER,
                        null reportable_state,
                        null source_state
                      from
                        (
                          select
                            r.*
                          from
                            IH_DW.RESULTS r,
                            (
                              select
                                distinct(requisition_id)
                              from
                                IH_DW.DW_ODS_ACTIVITY
                              where
                              --comment
                              --requisition_id ='98848VL'
                                --requisition_id in ('00506RW')
                                --requisition_id in ('8187HBL')
                                --requisition_id in ('35568UX')
                                --requisition_id in ('81113GL')
                                --requisition_id in ('27632GX','24122VX')
                                --requisition_id in ('650445L')
                                --requisition_id in ('7321PFL')
                                --requisition_id in ('02878ZX')
                                --requisition_id in ('9062FMF')
                                --requisition_id = '9831ZWF'
                                --uncomment
                                LAST_UPDATED_DATE >= v_start_time
                                
                                --((LAST_UPDATED_DATE >= v_start_time) and (last_updated_date < (SYSTIMESTAMP - INTERVAL '1' HOUR)))
                                
                                --LAST_UPDATED_DATE >= '13-FEB-19 03.00.00.000000000 PM'
                                --and last_updated_date < '14-FEB-19 03.00.00.000000000 AM'                                
                                
                                --LAST_UPDATED_DATE >= '13-FEB-19 10.50.00.000000000 AM'
                                --and last_updated_date < '13-FEB-19 03.00.00.000000000 PM'                                
                                
                                --LAST_UPDATED_DATE >= '07-NOV-18 12.00.00.000000000 AM'
                                --and last_updated_date < '07-NOV-18 11.00.00.000000000 PM'
                                --LAST_UPDATED_DATE >= '06-DEC-18 12.00.00.000000000 AM'
                                --and last_updated_date < '06-DEC-18 11.00.00.000000000 PM'                  
                                
                                --and last_updated_date < '25-MAR-18 12.00.00.000000000 AM' 
                                --LAST_UPDATED_DATE >= '01-APR-17 12.00.00.000000000 AM'
                                --and last_updated_date < '05-APR-17 12.00.00.000000000 AM'                  
                            ) a
                          where
                            r.requisition_id = a.requisition_id
                        ) re,
                        IH_DW.DIM_LAB_ORDER lo,
                        IH_DW.DIM_LAB_ORDER_DETAILS lod,
                        STATERPT_OWNER.PatientMaster p,
                        --staterpt_owner.gtt_pm p,
                        IH_DW.DIM_ACCOUNT a,
                        IH_DW.DIM_FACILITY f,
                        --IH_DW.DIM_PHYSICIAN dph,
                        IH_DW.DIM_LAB dl,
                        IH_DW.DIM_STAFF ds,
                        IH_DW.SPECTRA_MRN_ASSOCIATIONS asso	
                      where
                        lo.requisition_id = re.requisition_id
                        and re.LAB_ORDER_FK = lo.LAB_ORDER_PK
                        and lo.initiate_id = p.eid
                        and p.lab_fk = re.lab_fk
                        and re.LAB_ORDER_DETAILS_FK = lod.LAB_ORDER_DETAILS_PK
                        and lo.LAB_ORDER_PK = lod.LAB_ORDER_FK
                        and lo.account_fk = a.account_pk	
                        and a.facility_fk = f.facility_pk
                        --and lo.ORDERING_PHYSICIAN_NPI = dph.NPI
                        and re.lab_fk = dl.lab_pk
                        and lo.lab_fk = dl.lab_pk
                        and lo.SPECTRA_MRN_ASSC_FK = asso.SPECTRA_MRN_ASSC_pk
                        and ds.SPECTRA_MRN_FK = asso.SPECTRA_MRN_fK 
                        and ds.FACILITY_FK = asso.FACILITY_fK
                        and lod.TEST_CATEGORY in ('IMMUNO','IMMUN','PCR','ARUP');
                        --and lod.TEST_CATEGORY in ('IMMUNO','IMMUN','PCR','ARUP','HEMA');
                        --and p.state = p_state;
                        --and p.state in (p_state);
                        --and p.state in ('TX','AZ','CA','PA','OR','FL','WA');
                        --and p.state = 'CA';


            merge into 
                STATERPT_OWNER.GTT_RESULTS_EXTRACT dest
            using (
                  select
                  distinct(ACCESSION_NUMBER),
                  FACILITY_ID,
                  CID,
                  ETHNIC_GROUP,
                  PATIENT_RACE,
                  EXTERNAL_MRN,
                  PATIENT_LAST_NAME,
                  PATIENT_FIRST_NAME,
                  PATIENT_MIDDLE_NAME,
                  DATE_OF_BIRTH,
                  GENDER,
                  PATIENT_SSN,
                  NPI,
                  ORDERING_PHYSICIAN_NAME,
                  REPORT_NOTES,
                  SPECIMEN_RECEIVE_DATE,
                  COLLECTION_DATE,
                  COLLECTION_TIME,
                  COLLECTION_DATE_TIME,
                  DRAW_FREQ,
                  RES_RPRT_STATUS_CHNG_DT_TIME,
                  ORDER_DETAIL_STATUS,
                  ORDER_TEST_CODE,
                  ORDER_TEST_NAME,
                  RESULT_TEST_CODE,
                  RESULT_TEST_NAME,
                  RESULT_STATUS,
                  TEXTUAL_RESULT,
                  TEXTUAL_RESULT_FULL,
                  NUMERIC_RESULT,
                  UNITS,
                  REFERENCE_RANGE,
                  ABNORMAL_FLAG,
                  RELEASE_DATE_TIME,
                  --RESULT_COMMENTS,
                  trim(dbms_lob.substr( result_comments, 4000, 1 )) as result_comments,
                  PERFORMING_LAB_ID,
                  ORDER_METHOD,
                  SPECIMEN_SOURCE,
                  ORDER_NUMBER,
                  LOGGING_SITE,
                  AGE,
                  FACILITY_NAME,
                  COND_CODE,
                  PATIENT_TYPE,
                  SOURCE_OF_COMMENT,
                  PATIENT_ID,
                  ALTERNATE_PATIENT_ID,
                  REQUISITION_STATUS,
                  FACILITY_ADDRESS1,
                  FACILITY_ADDRESS2,
                  FACILITY_CITY,
                  FACILITY_STATE,
                  FACILITY_ZIP,
                  FACILITY_PHONE,
                  PATIENT_ACCOUNT_ADDRESS1,
                  PATIENT_ACCOUNT_ADDRESS2,
                  PATIENT_ACCOUNT_CITY,
                  PATIENT_ACCOUNT_STATE,
                  PATIENT_ACCOUNT_ZIP,
                  PATIENT_HOME_PHONE,
                  LOINC_CODE,
                  LOINC_NAME,
                  VALUE_TYPE,
                  EAST_WEST_FLAG,
                  INTERNAL_EXTERNAL_FLAG,
                  LAST_UPDATE_TIME,
                  SEQUENCE_NO,
                  FACILITY_ACCOUNT_STATUS,
                  FACILITY_ACTIVE_FLAG,
                  MICRO_ISOLATE,
                  MICRO_ORGANISM_NAME,
                  LAB_FK,
                  CLINICAL_MANAGER,
                  MEDICAL_DIRECTOR,
                  ACTI_FACILITY_ID,
                  FMC_NUMBER,
                  REPORTABLE_STATE
                  from
                      STATERPT_OWNER.GTT_STAFF_RESULTS_EXTRACT            
            ) src
            on (
            dest.accession_number = src.accession_number
            AND dest.facility_id = src.facility_id
            AND dest.patient_id = src.patient_id
            AND dest.order_test_name = src.order_test_name 
            AND dest.order_test_code = src.order_test_code 
            AND dest.textual_result_full = src.textual_result_full
            AND dest.last_update_time = src.last_update_time 
            )
            when MATCHED then
                update
                    set dest.cond_code = src.cond_code
            when NOT MATCHED then
            insert (
                  dest.ACCESSION_NUMBER,
                  dest.FACILITY_ID,
                  dest.CID,
                  dest.ETHNIC_GROUP,
                  dest.PATIENT_RACE,
                  dest.EXTERNAL_MRN,
                  dest.PATIENT_LAST_NAME,
                  dest.PATIENT_FIRST_NAME,
                  dest.PATIENT_MIDDLE_NAME,
                  dest.DATE_OF_BIRTH,
                  dest.GENDER,
                  dest.PATIENT_SSN,
                  dest.NPI,
                  dest.ORDERING_PHYSICIAN_NAME,
                  dest.REPORT_NOTES,
                  dest.SPECIMEN_RECEIVE_DATE,
                  dest.COLLECTION_DATE,
                  dest.COLLECTION_TIME,
                  dest.COLLECTION_DATE_TIME,
                  dest.DRAW_FREQ,
                  dest.RES_RPRT_STATUS_CHNG_DT_TIME,
                  dest.ORDER_DETAIL_STATUS,
                  dest.ORDER_TEST_CODE,
                  dest.ORDER_TEST_NAME,
                  dest.RESULT_TEST_CODE,
                  dest.RESULT_TEST_NAME,
                  dest.RESULT_STATUS,
                  dest.TEXTUAL_RESULT,
                  dest.TEXTUAL_RESULT_FULL,
                  dest.NUMERIC_RESULT,
                  dest.UNITS,
                  dest.REFERENCE_RANGE,
                  dest.ABNORMAL_FLAG,
                  dest.RELEASE_DATE_TIME,
                  dest.RESULT_COMMENTS,
                  dest.PERFORMING_LAB_ID,
                  dest.ORDER_METHOD,
                  dest.SPECIMEN_SOURCE,
                  dest.ORDER_NUMBER,
                  dest.LOGGING_SITE,
                  dest.AGE,
                  dest.FACILITY_NAME,
                  dest.COND_CODE,
                  dest.PATIENT_TYPE,
                  dest.SOURCE_OF_COMMENT,
                  dest.PATIENT_ID,
                  dest.ALTERNATE_PATIENT_ID,
                  dest.REQUISITION_STATUS,
                  dest.FACILITY_ADDRESS1,
                  dest.FACILITY_ADDRESS2,
                  dest.FACILITY_CITY,
                  dest.FACILITY_STATE,
                  dest.FACILITY_ZIP,
                  dest.FACILITY_PHONE,
                  dest.PATIENT_ACCOUNT_ADDRESS1,
                  dest.PATIENT_ACCOUNT_ADDRESS2,
                  dest.PATIENT_ACCOUNT_CITY,
                  dest.PATIENT_ACCOUNT_STATE,
                  dest.PATIENT_ACCOUNT_ZIP,
                  dest.PATIENT_HOME_PHONE,
                  dest.LOINC_CODE,
                  dest.LOINC_NAME,
                  dest.VALUE_TYPE,
                  dest.EAST_WEST_FLAG,
                  dest.INTERNAL_EXTERNAL_FLAG,
                  dest.LAST_UPDATE_TIME,
                  dest.SEQUENCE_NO,
                  dest.FACILITY_ACCOUNT_STATUS,
                  dest.FACILITY_ACTIVE_FLAG,
                  dest.MICRO_ISOLATE,
                  dest.MICRO_ORGANISM_NAME,
                  dest.LAB_FK,
                  dest.CLINICAL_MANAGER,
                  dest.MEDICAL_DIRECTOR,
                  dest.ACTI_FACILITY_ID,
                  dest.FMC_NUMBER,
                  dest.REPORTABLE_STATE            
            )values (
                  src.ACCESSION_NUMBER,
                  src.FACILITY_ID,
                  src.CID,
                  src.ETHNIC_GROUP,
                  src.PATIENT_RACE,
                  src.EXTERNAL_MRN,
                  src.PATIENT_LAST_NAME,
                  src.PATIENT_FIRST_NAME,
                  src.PATIENT_MIDDLE_NAME,
                  src.DATE_OF_BIRTH,
                  src.GENDER,
                  src.PATIENT_SSN,
                  src.NPI,
                  src.ORDERING_PHYSICIAN_NAME,
                  src.REPORT_NOTES,
                  src.SPECIMEN_RECEIVE_DATE,
                  src.COLLECTION_DATE,
                  src.COLLECTION_TIME,
                  src.COLLECTION_DATE_TIME,
                  src.DRAW_FREQ,
                  src.RES_RPRT_STATUS_CHNG_DT_TIME,
                  src.ORDER_DETAIL_STATUS,
                  src.ORDER_TEST_CODE,
                  src.ORDER_TEST_NAME,
                  src.RESULT_TEST_CODE,
                  src.RESULT_TEST_NAME,
                  src.RESULT_STATUS,
                  src.TEXTUAL_RESULT,
                  src.TEXTUAL_RESULT_FULL,
                  src.NUMERIC_RESULT,
                  src.UNITS,
                  src.REFERENCE_RANGE,
                  src.ABNORMAL_FLAG,
                  src.RELEASE_DATE_TIME,
                  src.RESULT_COMMENTS,
                  src.PERFORMING_LAB_ID,
                  src.ORDER_METHOD,
                  src.SPECIMEN_SOURCE,
                  src.ORDER_NUMBER,
                  src.LOGGING_SITE,
                  src.AGE,
                  src.FACILITY_NAME,
                  src.COND_CODE,
                  src.PATIENT_TYPE,
                  src.SOURCE_OF_COMMENT,
                  src.PATIENT_ID,
                  src.ALTERNATE_PATIENT_ID,
                  src.REQUISITION_STATUS,
                  src.FACILITY_ADDRESS1,
                  src.FACILITY_ADDRESS2,
                  src.FACILITY_CITY,
                  src.FACILITY_STATE,
                  src.FACILITY_ZIP,
                  src.FACILITY_PHONE,
                  src.PATIENT_ACCOUNT_ADDRESS1,
                  src.PATIENT_ACCOUNT_ADDRESS2,
                  src.PATIENT_ACCOUNT_CITY,
                  src.PATIENT_ACCOUNT_STATE,
                  src.PATIENT_ACCOUNT_ZIP,
                  src.PATIENT_HOME_PHONE,
                  src.LOINC_CODE,
                  src.LOINC_NAME,
                  src.VALUE_TYPE,
                  src.EAST_WEST_FLAG,
                  src.INTERNAL_EXTERNAL_FLAG,
                  src.LAST_UPDATE_TIME,
                  src.SEQUENCE_NO,
                  src.FACILITY_ACCOUNT_STATUS,
                  src.FACILITY_ACTIVE_FLAG,
                  src.MICRO_ISOLATE,
                  src.MICRO_ORGANISM_NAME,
                  src.LAB_FK,
                  src.CLINICAL_MANAGER,
                  src.MEDICAL_DIRECTOR,
                  src.ACTI_FACILITY_ID,
                  src.FMC_NUMBER,
                  src.PATIENT_ACCOUNT_STATE            
            );



			delete from 
				STATERPT_OWNER.GTT_RESULTS_EXTRACT 
			where  
				LENGTH(GTT_RESULTS_EXTRACT.RESULT_TEST_CODE) > 5
				OR LENGTH(GTT_RESULTS_EXTRACT.ACCESSION_NUMBER) < 9
				OR GTT_RESULTS_EXTRACT.result_comments like ('Conflicting or overlapping test%')
				OR GTT_RESULTS_EXTRACT.order_number IS NULL 
				OR GTT_RESULTS_EXTRACT.TEXTUAL_RESULT_FULL like 'CANCELLED%'
        OR GTT_RESULTS_EXTRACT.TEXTUAL_RESULT_FULL like 'CANCELED%'
				OR GTT_RESULTS_EXTRACT.TEXTUAL_RESULT_FULL like 'PENDING%';

      commit;

      update
        STATERPT_OWNER.GTT_RESULTS_EXTRACT
      set
        performing_lab_id = 'OUTSEND'
      where
        order_method in (
                  select
                    test_category
                  from  
                    IH_DW.LOV_TEST_CATEGORY
                  where
                    CUSTOM_DEPARTMENT = 'Sendout'    
        );
      commit; 
			
		END;	



    BEGIN
    
       delete from 
          STATERPT_OWNER.GTT_RESULTS_EXTRACT
        where
          (accession_number || order_number || order_test_code || result_test_code) in
          (
              select
                distinct(r.accession_number || r.order_number || r.order_test_code || r.result_test_code)
              from
                STATERPT_OWNER.GTT_RESULTS_EXTRACT r,
                STATERPT_OWNER.RESULTS_SENT_LOG l
              where
                r.order_number = l.order_number
                and r.accession_number = l.accession_number
                and r.order_test_code = l.order_test_code
                and r.result_test_code = l.result_test_code
                and r.release_date_time = l.release_date_time
          );
       commit;   
    END;


		
		BEGIN
/*    
      v_sql :=
			'select 
        *	
			from 
				--STATERPT_OWNER.TEMP_RESULTS_EXTRACT rs
        STATERPT_OWNER.GTT_RESULTS_EXTRACT rs
			where 
				PATIENT_ACCOUNT_STATE = ''' || p_state || ''' 
        --PATIENT_ACCOUNT_STATE in (' || p_state || ')
				--and accession_number in ( 
        and accession_number in ( 
					select 
						distinct(r.accession_number)
					from 
						--STATERPT_OWNER.TEMP_RESULTS_EXTRACT r,
            STATERPT_OWNER.GTT_RESULTS_EXTRACT r,
						( 
							select 
								distinct(accession_number)
							from 
								--STATERPT_OWNER.TEMP_RESULTS_EXTRACT
                STATERPT_OWNER.GTT_RESULTS_EXTRACT
							where 
								order_test_code = ''' || p_otc || ''' ';
                
                if(p_rtc is not null) then
                  v_sql := v_sql || ' and result_test_code = ''' || p_rtc || ''' ';
                end if;

                if(p_filter_inner is not null) then
                  v_sql := v_sql || ' and ' || p_filter_inner;
                end if;  
                
                v_sql := v_sql || ') p ';
  
        v_sql := v_sql || '  
					where 
						r.accession_number = p.accession_number ';                
          
            if(p_otc_outer is not null) then
              v_sql := v_sql || ' and r.order_test_code = ''' || p_otc_outer || ''' ';
            end if;
            
            if(p_rtc_outer is not null) then
              v_sql := v_sql || ' and r.result_test_code = ''' || p_rtc_outer || ''' ';
            end if;
            if(p_filter_outer is not null) then
              v_sql := v_sql || ' and ' || p_filter_outer;
            end if;  
            
            v_sql := v_sql || ' ) ';
            
				if(p_otc_close is not null) then
          v_sql := v_sql || ' and rs.order_test_code = ''' || p_otc_close || '''  ';
        end if;
        
        if(p_rtc_close is not null) then
          v_sql := v_sql || ' and rs.result_test_code like ''' || p_rtc_close || ''' ';
        end if;
        
        if(p_filter_ew is not null) then
          v_sql := v_sql || ' and ' || p_filter_ew;
        end if;        
   
        --dbms_output.enable();

        dbms_output.put_line('p_dto.state: ' || p_state);
        dbms_output.put_line('p_dto.otc: ' || p_otc);
        dbms_output.put_line('p_dto.rtc: ' || p_rtc);
        dbms_output.put_line('p_dto.filter_inner: ' || p_filter_inner);
        dbms_output.put_line('p_dto.otc_outer: ' || p_otc_outer);
        dbms_output.put_line('p_dto.rtc_outer: ' || p_rtc_outer);
        dbms_output.put_line('p_dto.filter_outer: ' || p_filter_outer);
        dbms_output.put_line('p_dto.otc_close: ' || p_otc_close);
        dbms_output.put_line('p_dto.rtc_close: ' || p_rtc_close);
        dbms_output.put_line('p_dto.filter_ew: ' || p_filter_ew);
        
        dbms_output.put_line('v_sql: ' || v_sql);
*/        
        select
          count(*)
        into
          v_gtt_count
        from
          --STATERPT_OWNER.TEMP_RESULTS_EXTRACT;
          STATERPT_OWNER.GTT_RESULTS_EXTRACT;
          
        dbms_output.put_line('v_gtt_count: ' || v_gtt_count); 
        
        select v_gtt_count into p_gtt_count from dual;
        
        
        
        
        
        SP_ASR_FILTER_RESULTS( 
          p_state => p_state, 
          p_gtt_count => v_gtt_count
        );

        dbms_output.put_line('v_gtt_count: ' || v_gtt_count);
        select v_gtt_count into p_gtt_count from dual;
        
        
         EXECUTE IMMEDIATE 'truncate table STATERPT_OWNER.GTT_RESULTS_EXTRACT';
         COMMIT;


        for filtered_sendout in (
          select
            *
          from
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT 
          where 
            source_state = 'sendout'            
        ) loop
            begin
                v_noaddr_order_number := null;
                v_noaddr_accession_number := null;
                v_noaddr_otc := null;
                v_noaddr_rtc := null;
                
                select
                    order_number,
                    accession_number,
                    order_test_code,
                    result_test_code
                into
                    v_noaddr_order_number,
                    v_noaddr_accession_number,
                    v_noaddr_otc,
                    v_noaddr_rtc
                from
                    STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT
                where
                    order_number = filtered_sendout.order_number
                    and accession_number = filtered_sendout.accession_number
                    and order_test_code = filtered_sendout.order_test_code
                    and result_test_code = filtered_sendout.result_test_code
                    and source_state = 'noaddr';

                if(v_noaddr_order_number is not null) then
                    --dbms_output.put_line('v_noaddr_order_number: ' || v_noaddr_order_number);
                    --dbms_output.put_line('v_noaddr_accession_number: ' || v_noaddr_accession_number);
                    --dbms_output.put_line('v_noaddr_otc: ' || v_noaddr_otc);
                    --dbms_output.put_line('v_noaddr_rtc: ' || v_noaddr_rtc);                
                  
                    delete from
                        STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT
                    where
                        order_number = v_noaddr_order_number
                        and accession_number = v_noaddr_accession_number
                        and order_test_code = v_noaddr_otc
                        and result_test_code = v_noaddr_rtc                        
                        and source_state = 'noaddr';
                    commit;    
                end if;
                EXCEPTION when 
                  no_data_found then 
                    v_noaddr_order_number := null;                
            end;
        end loop;


        for filtered_sendout in (
          select
            *
          from
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT 
          where 
            source_state = 'facility'            
        ) loop
            begin
                v_noaddr_order_number := null;
                v_noaddr_accession_number := null;
                v_noaddr_otc := null;
                v_noaddr_rtc := null;
                
                select
                    order_number,
                    accession_number,
                    order_test_code,
                    result_test_code
                into
                    v_noaddr_order_number,
                    v_noaddr_accession_number,
                    v_noaddr_otc,
                    v_noaddr_rtc
                from
                    STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT
                where
                    order_number = filtered_sendout.order_number
                    and accession_number = filtered_sendout.accession_number
                    and order_test_code = filtered_sendout.order_test_code
                    and result_test_code = filtered_sendout.result_test_code
                    and source_state = 'noaddr';

                if(v_noaddr_order_number is not null) then
                    --dbms_output.put_line('v_noaddr_order_number: ' || v_noaddr_order_number);
                    --dbms_output.put_line('v_noaddr_accession_number: ' || v_noaddr_accession_number);
                    --dbms_output.put_line('v_noaddr_otc: ' || v_noaddr_otc);
                    --dbms_output.put_line('v_noaddr_rtc: ' || v_noaddr_rtc);                
                  
                    delete from
                        STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT
                    where
                        order_number = v_noaddr_order_number
                        and accession_number = v_noaddr_accession_number
                        and order_test_code = v_noaddr_otc
                        and result_test_code = v_noaddr_rtc                        
                        and source_state = 'noaddr';
                    commit;    
                end if;
                EXCEPTION when 
                  no_data_found then 
                    v_noaddr_order_number := null;                
            end;
        end loop;

/*
        insert into 
          STATERPT_OWNER.GTT_RESULTS_EXTRACT
          select 
            * 
          from 
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT;
*/            
            
            
        insert into 
            STATERPT_OWNER.GTT_RESULTS_EXTRACT
            select 
                * 
            from 
                STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT
            where
                ACTI_FACILITY_ID not in (
                    select
                        facility_id
                    from
                        STATERPT_OWNER.TEST_FACILITIES
                    where
                        status = 'active'
                );            
            
            
            

/*         
        insert into 
          STATERPT_OWNER.GTT_RESULTS_EXTRACT
          select 
            * 
          from 
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT 
          where 
            source_state = 'patient';
            
        insert into 
          STATERPT_OWNER.GTT_RESULTS_EXTRACT
          select 
            * 
          from 
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT 
          where 
            source_state = 'facility'; 
            
            
        for filtered_sendout in (
          select
            *
          from
            STATERPT_OWNER.GTT_FILTER_RESULTS_EXTRACT 
          where 
            source_state = 'sendout'            
        ) loop
          --dbms_output.put_line('filtered_sendout.order_number: ' || filtered_sendout.order_number);
          
          begin
            dbms_output.put_line('filtered_sendout.order_number: ' || filtered_sendout.order_number);

            
//            select
//              count(*)
//            into
//              v_gtt_filter_count
//            from
//              STATERPT_OWNER.INTRA_LABS_SENDOUT_NO_DEMO;
//            
//            select
//              distinct(order_number)
//            into
//              v_order_number
//            from
//              STATERPT_OWNER.INTRA_LABS_SENDOUT_NO_DEMO
//            where
//              order_number = filtered_sendout.order_number;
//
//            dbms_output.put_line('v_order_number: ' || v_order_number);
            
            
            select
              count(*)
            into
              v_gtt_filter_count              
            from
              (
	            select
	              distinct(order_number)
	            from
	              STATERPT_OWNER.INTRA_LABS_SENDOUT_NO_DEMO
	            where
	            	order_number = filtered_sendout.order_number                
              );            
            
              
            --if((v_order_number is null) or (length(ltrim(rtrim(v_order_number)))) = 0) then
            --if((v_gtt_filter_count = 0) or (v_order_number is null)) then
            if((v_gtt_filter_count = 0)) then
              insert into 
                STATERPT_OWNER.INTRA_LABS_SENDOUT_NO_DEMO
              (  
                  ACCESSION_NUMBER,
                  FACILITY_ID,
                  CID,
                  ETHNIC_GROUP,
                  PATIENT_RACE,
                  EXTERNAL_MRN,
                  PATIENT_LAST_NAME,
                  PATIENT_FIRST_NAME,
                  PATIENT_MIDDLE_NAME,
                  DATE_OF_BIRTH,
                  GENDER,
                  PATIENT_SSN,
                  NPI,
                  ORDERING_PHYSICIAN_NAME,
                  REPORT_NOTES,
                  SPECIMEN_RECEIVE_DATE,
                  COLLECTION_DATE,
                  COLLECTION_TIME,
                  COLLECTION_DATE_TIME,
                  DRAW_FREQ,
                  RES_RPRT_STATUS_CHNG_DT_TIME,
                  ORDER_DETAIL_STATUS,
                  ORDER_TEST_CODE,
                  ORDER_TEST_NAME,
                  RESULT_TEST_CODE,
                  RESULT_TEST_NAME,
                  RESULT_STATUS,
                  TEXTUAL_RESULT,
                  TEXTUAL_RESULT_FULL,
                  NUMERIC_RESULT,
                  UNITS,
                  REFERENCE_RANGE,
                  ABNORMAL_FLAG,
                  RELEASE_DATE_TIME,
                  RESULT_COMMENTS,
                  PERFORMING_LAB_ID,
                  ORDER_METHOD,
                  SPECIMEN_SOURCE,
                  ORDER_NUMBER,
                  LOGGING_SITE,
                  AGE,
                  FACILITY_NAME,
                  COND_CODE,
                  PATIENT_TYPE,
                  SOURCE_OF_COMMENT,
                  PATIENT_ID,
                  ALTERNATE_PATIENT_ID,
                  REQUISITION_STATUS,
                  FACILITY_ADDRESS1,
                  FACILITY_ADDRESS2,
                  FACILITY_CITY,
                  FACILITY_STATE,
                  FACILITY_ZIP,
                  FACILITY_PHONE,
                  PATIENT_ACCOUNT_ADDRESS1,
                  PATIENT_ACCOUNT_ADDRESS2,
                  PATIENT_ACCOUNT_CITY,
                  PATIENT_ACCOUNT_STATE,
                  PATIENT_ACCOUNT_ZIP,
                  PATIENT_HOME_PHONE,
                  LOINC_CODE,
                  LOINC_NAME,
                  VALUE_TYPE,
                  EAST_WEST_FLAG,
                  INTERNAL_EXTERNAL_FLAG,
                  LAST_UPDATE_TIME,
                  SEQUENCE_NO,
                  FACILITY_ACCOUNT_STATUS,
                  FACILITY_ACTIVE_FLAG,
                  MICRO_ISOLATE,
                  MICRO_ORGANISM_NAME,
                  LAB_FK,
                  CLINICAL_MANAGER,
                  MEDICAL_DIRECTOR,
                  ACTI_FACILITY_ID,
                  FMC_NUMBER,
                  REPORTABLE_STATE,
                  SOURCE_STATE,
                  NOTIFIED_FLAG,
                  NOTIFIED_TIME
              )values(
                  filtered_sendout.ACCESSION_NUMBER,
                  filtered_sendout.FACILITY_ID,
                  filtered_sendout.CID,
                  filtered_sendout.ETHNIC_GROUP,
                  filtered_sendout.PATIENT_RACE,
                  filtered_sendout.EXTERNAL_MRN,
                  filtered_sendout.PATIENT_LAST_NAME,
                  filtered_sendout.PATIENT_FIRST_NAME,
                  filtered_sendout.PATIENT_MIDDLE_NAME,
                  filtered_sendout.DATE_OF_BIRTH,
                  filtered_sendout.GENDER,
                  filtered_sendout.PATIENT_SSN,
                  filtered_sendout.NPI,
                  filtered_sendout.ORDERING_PHYSICIAN_NAME,
                  filtered_sendout.REPORT_NOTES,
                  filtered_sendout.SPECIMEN_RECEIVE_DATE,
                  filtered_sendout.COLLECTION_DATE,
                  filtered_sendout.COLLECTION_TIME,
                  filtered_sendout.COLLECTION_DATE_TIME,
                  filtered_sendout.DRAW_FREQ,
                  filtered_sendout.RES_RPRT_STATUS_CHNG_DT_TIME,
                  filtered_sendout.ORDER_DETAIL_STATUS,
                  filtered_sendout.ORDER_TEST_CODE,
                  filtered_sendout.ORDER_TEST_NAME,
                  filtered_sendout.RESULT_TEST_CODE,
                  filtered_sendout.RESULT_TEST_NAME,
                  filtered_sendout.RESULT_STATUS,
                  filtered_sendout.TEXTUAL_RESULT,
                  filtered_sendout.TEXTUAL_RESULT_FULL,
                  filtered_sendout.NUMERIC_RESULT,
                  filtered_sendout.UNITS,
                  filtered_sendout.REFERENCE_RANGE,
                  filtered_sendout.ABNORMAL_FLAG,
                  filtered_sendout.RELEASE_DATE_TIME,
                  filtered_sendout.RESULT_COMMENTS,
                  filtered_sendout.PERFORMING_LAB_ID,
                  filtered_sendout.ORDER_METHOD,
                  filtered_sendout.SPECIMEN_SOURCE,
                  filtered_sendout.ORDER_NUMBER,
                  filtered_sendout.LOGGING_SITE,
                  filtered_sendout.AGE,
                  filtered_sendout.FACILITY_NAME,
                  filtered_sendout.COND_CODE,
                  filtered_sendout.PATIENT_TYPE,
                  filtered_sendout.SOURCE_OF_COMMENT,
                  filtered_sendout.PATIENT_ID,
                  filtered_sendout.ALTERNATE_PATIENT_ID,
                  filtered_sendout.REQUISITION_STATUS,
                  filtered_sendout.FACILITY_ADDRESS1,
                  filtered_sendout.FACILITY_ADDRESS2,
                  filtered_sendout.FACILITY_CITY,
                  filtered_sendout.FACILITY_STATE,
                  filtered_sendout.FACILITY_ZIP,
                  filtered_sendout.FACILITY_PHONE,
                  filtered_sendout.PATIENT_ACCOUNT_ADDRESS1,
                  filtered_sendout.PATIENT_ACCOUNT_ADDRESS2,
                  filtered_sendout.PATIENT_ACCOUNT_CITY,
                  filtered_sendout.PATIENT_ACCOUNT_STATE,
                  filtered_sendout.PATIENT_ACCOUNT_ZIP,
                  filtered_sendout.PATIENT_HOME_PHONE,
                  filtered_sendout.LOINC_CODE,
                  filtered_sendout.LOINC_NAME,
                  filtered_sendout.VALUE_TYPE,
                  filtered_sendout.EAST_WEST_FLAG,
                  filtered_sendout.INTERNAL_EXTERNAL_FLAG,
                  filtered_sendout.LAST_UPDATE_TIME,
                  filtered_sendout.SEQUENCE_NO,
                  filtered_sendout.FACILITY_ACCOUNT_STATUS,
                  filtered_sendout.FACILITY_ACTIVE_FLAG,
                  filtered_sendout.MICRO_ISOLATE,
                  filtered_sendout.MICRO_ORGANISM_NAME,
                  filtered_sendout.LAB_FK,
                  filtered_sendout.CLINICAL_MANAGER,
                  filtered_sendout.MEDICAL_DIRECTOR,
                  filtered_sendout.ACTI_FACILITY_ID,
                  filtered_sendout.FMC_NUMBER,
                  filtered_sendout.REPORTABLE_STATE,
                  filtered_sendout.SOURCE_STATE,
                  'N',
                  null            
              );
                  
              COMMIT;
            end if;
            
            EXCEPTION when no_data_found
              then null; --or whatever you need here            
            
          end;  
        end loop;                
*/        
        
        --dbms_output.disable();
/*        
        select v_sql into p_sql from dual;
        
        OPEN p_recordset FOR v_sql;
*/        
        --open p_recordset for select * from temp_results_extract;
        
				EXCEPTION
				WHEN table_or_view_not_exist THEN
					dbms_output.put_line('Table STATERPT_OWNER.GTT_RESULTS_EXTRACT did not exist at time of truncate. Continuing....');

				WHEN attempted_ddl_on_in_use_GTT THEN
					dbms_output.put_line('STATERPT_OWNER.GTT_RESULTS_EXTRACT is in use. Commit!');
					raise;

				WHEN OTHERS THEN
					DBMS_OUTPUT.put_line ('Error in creating table STATERPT_OWNER.GTT_RESULTS_EXTRACT');
					DBMS_OUTPUT.put_line('v_error:'||sqlcode);
					DBMS_OUTPUT.put_line('v_sqlerrm:'||sqlerrm);
          dbms_output.put_line('Backtrace => '||dbms_utility.format_error_backtrace);
          dbms_output.put_line('SQLCODE => '||SQLCODE);           
					if sqlcode = -60 then -- deadlock error is ORA-00060
					  null;
					else
					  raise;
					end if;		
		END;
	
END SP_ASR_PROC_TRACK_RESULTS;