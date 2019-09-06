USE [ICM_Stage]
GO

/****** Object:  StoredProcedure [ICMTRANS_PEACETX].[usp_MergePremiseAddressDetails_ODS]    Script Date: 7/25/2019 7:01:24 PM ******/
DROP PROCEDURE [ICMTRANS_PEACETX].[usp_MergePremiseAddressDetails_ODS]
GO

/****** Object:  StoredProcedure [ICMTRANS_PEACETX].[usp_MergePremiseAddressDetails_ODS]    Script Date: 7/25/2019 7:01:24 PM ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROCEDURE [ICMTRANS_PEACETX].[usp_MergePremiseAddressDetails_ODS]
  @LoadControlID int
AS

BEGIN

  SET NOCOUNT ON;

  DECLARE
    @dtNow datetime = GETDATE()
    ,@ExpectedRowCount int = 0
    ,@NewRowCount int = 0
    ,@ChangedRowCount int = 0
  ;

  -- Build resultset of latest view of all unique DEBTORNUM and PREMNUM combinations.
  -- This is patterned from PREM_DTLS logic.
  IF OBJECT_ID('tempdb..#cust_prem') IS NOT NULL
    DROP TABLE #cust_prem;


  BEGIN TRANSACTION premAddrDtls;

  BEGIN TRY

    -- Update HIS_IND of existing ICM_ODS.ICM_ODS.PREM_ADDR_DTLS that needs to be historied.
    -- These rows have matching key columns in #prem_addr_dtls_withChanges.

    -- Perform MERGE for new records
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

    SELECT @ChangedRowCount =@@ROWCOUNT;

    COMMIT TRANSACTION premAddrDtls
    ;
  END TRY

  BEGIN CATCH
    DECLARE
      @ErrorMessage nvarchar(4000)
      ,@ErrorSeverity int
      ,@ErrorState int
    ;
    SELECT
      @ErrorMessage = ERROR_MESSAGE()
      ,@ErrorSeverity = ERROR_SEVERITY()
      ,@ErrorState = ERROR_STATE()
    ;
    ROLLBACK TRANSACTION premAddrDtls;

    -- Use RAISERROR inside the CATCH block to return error
    -- information about the original error that caused
    -- execution to jump to the CATCH block.
    RAISERROR (
      @ErrorMessage -- Message text.
      ,@ErrorSeverity -- Severity.
      ,@ErrorState -- State.
      );
  END CATCH
END
GO

