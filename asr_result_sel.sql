SELECT distinct(re.ACCESSION_NUMBER) accession_no,
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
 
  	FROM ih_dw.results re,

       ih_dw.dw_ods_activity act,

       ih_dw.dim_lab_order lo,

       ih_dw.dim_lab_order_details lod,

       staterpt_owner.patientmaster p,

       ih_dw.dim_account a,

       ih_dw.dim_facility f,

       ih_dw.dim_lab dl,

       ih_dw.dim_patient dp,

       ih_dw.spectra_mrn_associations asso

      WHERE act.last_updated_date >= TO_DATE ('21-OCT-19 03.00 PM', 'DD-MON-RR HH:MI AM') -- i have put the date in the date wrapper

   AND act.requisition_id = re.requisition_id --WE CAN DO THIS JOIN BECUASE REQUISITION_ID IS THE UNIQUE KEY IN ACTIVITY TABLE and this join will not give duplicates

   AND lo.requisition_id = act.requisition_id

   AND lo.lab_order_pk = lod.lab_order_fk

   AND re.lab_order_details_fk = lod.lab_order_details_pk

   AND re.lab_order_fk = lo.lab_order_pk

   AND p.eid = lo.initiate_id 

   AND p.lab_fk = lo.lab_fk

   AND lo.account_fk = a.account_pk

   AND a.facility_fk = f.facility_pk

   AND re.lab_fk = dl.lab_pk

   AND lo.lab_fk = dl.lab_pk

   AND lo.spectra_mrn_assc_fk = asso.spectra_mrn_assc_pk

   AND dp.spectra_mrn_fk = asso.spectra_mrn_fk

   AND dp.facility_fk = asso.facility_fk

   AND lod.test_category IN ('IMMUNO', 'IMMUN', 'PCR', 'ARUP');