    MERGE ICM_ODS.ICM_ODS.PREM_ADDR_DTLS AS a
      USING
        (
        SELECT
          CIS_CUST_NO
          ,CIS_PREM_NO
          ,CONVERT(tinyint,1) AS SRC_SYS_ID
          ,SVC_STRT_NO
          ,SVC_STRT_NAME
          ,SVC_UNIT_NO
          ,SVC_CITY
          ,SVC_STATE
          ,SVC_ZIP
          ,SVC_CNTRY
          FROM #prem_addr_prelim
        ) AS b
      ON
        (
          a.CIS_PREM_NO = b.CIS_PREM_NO
        AND a.CIS_CUST_NO = b.CIS_CUST_NO
        AND a.SRC_SYS_ID = b.SRC_SYS_ID
        )

      WHEN NOT MATCHED THEN
        INSERT
          (
          CIS_CUST_NO
          ,CIS_PREM_NO
          ,SRC_SYS_ID
          ,SVC_STRT_NO
          ,SVC_STRT_NAME
          ,SVC_UNIT_NO
          ,SVC_CITY
          ,SVC_STATE
          ,SVC_ZIP
          ,SVC_CNTRY
          ,HIS_IND
          ,LOADCONTROLCREATEID
          ,CRDT
          ,UPDT
          )
          VALUES
          (
          b.CIS_CUST_NO
          ,b.CIS_PREM_NO
          ,b.SRC_SYS_ID
          ,b.SVC_STRT_NO
          ,b.SVC_STRT_NAME
          ,b.SVC_UNIT_NO
          ,b.SVC_CITY
          ,b.SVC_STATE
          ,b.SVC_ZIP
          ,b.SVC_CNTRY
          ,'C'
          ,@LoadControlID
          ,@dtNow
          ,@dtNow
          );


    MERGE ICM_ODS.ICM_ODS.PREM_DTLS AS a
      USING
        (
        SELECT 
          CIS_CUST_NO
          ,CIS_PREM_NO
          ,LDC_ACNT_NO
          ,SRC_SYS_ID
          ,LDC_CD
          ,SIGN_DT
          ,FLOW_BEGIN_DT
          ,FLOW_END_DT
          ,DW_STATUS
          ,DW_SUB_STATUS
          ,SRC_STATUS
          ,COMMDTY_CD
          ,COMPANY_CD
          ,BRAND
          ,BUS_UNIT
          ,LOB
          ,CUST_TYPE_CD
          ,DWLNG_TYPE
          ,PLAN_ID
          ,PRODUCT_CD
          ,PLAN_OFFER_TYPE
          ,RATE_PLAN_BEGIN_DT
          ,RATE_PLAN_END_DT
          ,PLAN_TERM
          FROM #PREM_DTLS_PEACE
        ) AS b
        ON
        (
            a.CIS_PREM_NO = b.CIS_PREM_NO
        AND a.CIS_CUST_NO = b.CIS_CUST_NO
        AND a.SRC_SYS_ID = b.SRC_SYS_ID
        AND a.SRC_SYS_ID = 1
        AND a.HIS_IND = 'C'
        )

      WHEN NOT MATCHED  BY TARGET THEN
        -- Insert new rows
        -- Note: These rows contain not only new 'C' but 'H' whose JOIN keys 
        --       do not exist yet in target table ICM_ODS.PREM_DTLS.
        INSERT
          (
          CIS_CUST_NO
          ,CIS_PREM_NO
          ,LDC_ACNT_NO
          ,SRC_SYS_ID
          ,LDC_CD
          ,CRDT
          ,SIGN_DT
          ,FLOW_BEGIN_DT
          ,FLOW_END_DT
          ,DW_STATUS
          ,DW_SUB_STATUS
          ,SRC_STATUS
          ,COMMDTY_CD
          ,COMPANY_CD
          ,BRAND
          ,BUS_UNIT
          ,LOB
          ,CUST_TYPE_CD
          ,DWLNG_TYPE
          ,PLAN_ID
          ,PRODUCT_CD
          ,PLAN_OFFER_TYPE
          ,RATE_PLAN_BEGIN_DT
          ,RATE_PLAN_END_DT
          ,PLAN_TERM
          ,HIS_IND
          ,LOADCONTROLCREATEID
          ,UPDT
          )
        VALUES
          (
          b.CIS_CUST_NO
          ,b.CIS_PREM_NO
          ,b.LDC_ACNT_NO
          ,b.SRC_SYS_ID
          ,b.LDC_CD
          ,@dtNow
          ,b.SIGN_DT
          ,b.FLOW_BEGIN_DT
          ,b.FLOW_END_DT
          ,b.DW_STATUS
          ,b.DW_SUB_STATUS
          ,b.SRC_STATUS
          ,b.COMMDTY_CD
          ,b.COMPANY_CD
          ,b.BRAND
          ,b.BUS_UNIT
          ,b.LOB
          ,b.CUST_TYPE_CD
          ,b.DWLNG_TYPE
          ,b.PLAN_ID
          ,b.PRODUCT_CD
          ,b.PLAN_OFFER_TYPE
          ,b.RATE_PLAN_BEGIN_DT
          ,b.RATE_PLAN_END_DT
          ,b.PLAN_TERM
          ,'C'
          ,@LoadControlID
          ,@dtNow );


    MERGE ICM_ODS.ICM_ODS.PREM_ENROL_DTLS AS ods  
      USING  
        (  
   SELECT [CIS_CUST_NO]  
      ,[CIS_PREM_NO]  
     ,AGENT_CD,  
     CUST_GRP_CD,  
     GROUP_ID,  
     PROMO_ID,
     MDU_IND,
     MSID
      ,[LOADCONTROLCREATEID]  
      ,[CRDT] 
   FROM  [ICM_Stage].[ICM_TRANS].[PEACETX_PREM_ENROL_CHANL_DTLS]  
        ) AS b  
        ON b.CIS_PREM_NO = ods.CIS_PREM_NO  
  AND b.CIS_CUST_NO = ods.CIS_CUST_NO  
  AND ods.SRC_SYS_ID = 1  AND ods.HIS_IND = 'C'  
  
  
      WHEN NOT MATCHED THEN  
        INSERT  
          (  
   CIS_CUST_NO,  
   CIS_PREM_NO,  
   SRC_SYS_ID,  
   AGENT_CD,  
   APPSRC_CUSTGRP_CD,  
   GROUP_ID,  
   PROMO_ID,
   MDU_IND,
   MSID,  
   HIS_IND,  
   LOADCONTROLCREATEID,  
   CRDT,  
   UPDT  
          )  
          VALUES  
          (  
      b.CIS_CUST_NO  
     ,b.CIS_PREM_NO  
     ,1  
    ,AGENT_CD,  
     CUST_GRP_CD,  
    GROUP_ID,  
    PROMO_ID,
    MDU_IND,
    MSID
     ,'C'  
     ,@LoadControlID  
     ,@dtNow  
     ,@dtNow  
          ) ;