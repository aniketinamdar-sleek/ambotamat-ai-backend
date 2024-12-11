def generate_query(staff_grouping):
    return f"""
WITH xero_client AS (
  SELECT 
    uen, 
    MAX(CASE WHEN recon_24h > 0 THEN day END ) AS recon_xero 
  FROM `bigquery-production-309309.tableau.tb_portfolio_leads_accounting`
  GROUP BY 1
)

SELECT  
  CompanyName AS company_name,
  companyid AS company_id,
  uen,
  plan,
  next_fye_to_file,
  accountant,
  accounting_clients_software,
  latest_created_receipt_creation_dt AS latest_created_receipt_date,
  latest_bank_statement_bsm_creation_dt AS latest_bank_statement_bsm_date,
  sb_stock_l_3mos + sb_stock_m_3mos AS unreconciled_statements_sleekbooks,
  xero_stock_l_3mos + xero_stock_m_3mos AS unreconciled_statements_xero,
  eot.internalname as eot_extension,
FROM (
  SELECT b.*,
    CASE 
      WHEN LOWER(accounting_software) = 'sleekbooks' 
        AND company_accounting_status= 'Inscope' 
        AND active_or_not = 'Active'
        AND DATE(sb_max_trans_dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
        AND IFNULL(sb_stock_m_3mos,0) = 0
      THEN 'Active and up to date'

      WHEN (accounting_software = 'xero' OR accounting_software = 'xero (sleek)')
        AND company_accounting_status = 'Inscope'
        AND active_or_not = 'Active'
        AND (DATE(max_statement) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
             OR DATE(transactiondate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH))
        AND IFNULL(xero_stock_m_3mos,0) = 0
      THEN 'Active and up to date'

      WHEN active_or_not = 'Active' 
        AND company_accounting_status = 'Inscope'  
      THEN 'Active not up to date'
    END AS active_category_breakdown,

    CASE 
      WHEN active_or_not = 'Not Active'
        AND LOWER(accounting_software)='no software matched' 
        OR accounting_software IS NULL
      THEN 'No Accounting Software'

      WHEN active_or_not = 'Not Active' 
        AND LOWER(accounting_software) = 'sleekbooks'
        AND sb_max_trans_dt IS NULL
      THEN 'With Accounting Software but no BT'

      WHEN active_or_not = 'Not Active' 
        AND accounting_software IN ('xero','xero (sleek)')
        AND transactiondate IS NULL
      THEN 'With Accounting Software but no BT'

      WHEN active_or_not ='Not Active' 
      THEN 'Bank Transactin is Past 2 Months'
    END AS not_active_category_breakdown

  FROM (
    SELECT a.*,
      CASE 
        WHEN accounting_software = 'sleekbooks'
          AND company_accounting_status = 'Inscope' 
          AND (DATE(sb_max_trans_dt) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
               OR DATE(r_max_createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
               OR DATE(bsm_createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH))  
        THEN 'Active'

        WHEN (accounting_software = 'xero' OR accounting_software = 'xero (sleek)')
          AND company_accounting_status = 'Inscope'
          AND (DATE(transactiondate) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
               OR DATE(max_statement) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
               OR DATE(r_max_createdat) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)
               OR DATE(bsm_createdat) >=DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH)) 
        THEN 'Active'  
        ELSE 'Not Active' 
      END AS active_or_not

    FROM (
      SELECT  
        a.CompanyName,
        a.CompanyID,
        IFNULL(b.exemption,'No') AS exemption,
        b.reason_exemption,
        IFNULL(b.is_core_client,'No') AS is_core_client,
        a.entity_code,
        a.UEN,
        a.CompanyStatus AS Status,
        CASE 
          WHEN tier='accounting-annual-filling' THEN 'Yearly' 
          WHEN tier='accounting-payroll' THEN 'Monthly'
          WHEN tier='accounting-premium' THEN 'Monthly'
          WHEN tier='accounting-annual-filling,accounting-payroll' THEN 'Both Monthly and Yearly'
          WHEN tier='accounting-payroll,accounting-annual-filling' THEN 'Both Monthly and Yearly'
          ELSE 'None'
        END AS plan,
        a.accounting_client_status,
        a.createdat,
        a.incorporation_date,
        a.last_filed_fye,
        a.next_fye_to_file,
        CASE WHEN DATE(day) = CURRENT_DATE() THEN invoice_amount_re END AS invoice_amount_re,
        CASE WHEN DATE(day) = CURRENT_DATE() THEN invoice_amount_re_acc END AS invoice_amount_re_nonacc,
        CASE WHEN DATE(day) = CURRENT_DATE() THEN invoice_amount_ot END AS invoice_amount_ot,
        CASE WHEN DATE(day) = CURRENT_DATE() THEN invoice_amount_ot_disc END AS invoice_amount_ot_disc,
        a.receipts_in_inbox_coding_engine,
        a.receipts_in_inbox_dext,
        a.receipts_in_inbox_hubdoc,
        a.doc_count,
        a.l_1_month_bucket,
        a.b_1_3_months_bucket,
        a.b_3_6_months_bucker,
        a.b_6_12_months_bucket,
        a.m_12_months_bucket,
        a.sb_stock_l_3mos,
        a.xero_stock_l_3mos,
        CASE WHEN a.nd_cnt_active > 0 THEN 'Yes' Else 'No' END AS ND,
        DATE(b.cessation_date) AS cessation_date,
        a.ptf_lead_name,
        a.a_tl_name AS accountant,
        CASE 
          WHEN a.accounting_software = 'sleekbooks' THEN 'Sleekbooks'
          WHEN a.accounting_software='xero (client)' THEN 'Xero Client'
          WHEN a.accounting_software IN ('xero (sleek)','xero') THEN 'Xero' 
          WHEN a.accounting_software IS NULL OR a.accounting_software = 'no software matched' THEN 'No Software' 
        END AS accounting_clients_software,
        CASE 
          WHEN a.incorp_l_6mos = true 
            AND a.CompanyStatus = 'live'
          THEN 'Recently Incorporated (3 Months ago)'
          
          WHEN a.CompanyStatus IN (
            'processing_incorp_transfer','live_post_incorporation','paid_and_awaiting_company_detail',
            'draft','processing_by_sleek','processing_by_companies_house','referred_to_acra','paid_and_incomplete'
          ) THEN 'Not Incorp / Pending Transfer'
          
          WHEN a.dormant = true OR a.adhoc = true THEN 'Adhoc (One Time) + Dormant'
          
          WHEN LOWER(a.CompanyStatus) IN ('striking_off_requested','strikng_off','churn_requested','churn_process') 
          THEN 'SO + Churned'
          
          ELSE 'Inscope' 
        END company_accounting_status,
        accounting_software,
        r_max_createdat,
        bsm_createdat,
        transactiondate,
        max_statement,
        sb_max_trans_dt,
        sb_stock_m_3mos,
        xero_stock_m_3mos,
        r_max_createdat AS latest_created_receipt_creation_dt,
        bsm_createdat AS latest_bank_statement_bsm_creation_dt,
        sb_max_trans_dt AS latest_bank_transaction_sb_transaction_dt,
        sb_max_recon_trans_dt AS latest_reconciliation_sb_transaction_dt,
        transactiondate AS latest_bank_transaction_xero_account_transaction,
        max_statement AS latest_bank_transaction_xero_ui_path,
        recon_xero AS latest_reconciliation_date_xero_xero_client

      FROM `bigquery-production-309309.tableau.tb_portfolio_leads_accounting` a
      LEFT JOIN `bigquery-production-309309.zoho_webhook.vw_wh_core_accounting_clients` b 
        ON (a.CompanyID=b.companyid AND a.entity_code = b.entitycode)
      LEFT JOIN xero_client xc ON (a.uen = xc.uen)
      WHERE a.day = CURRENT_DATE()
        AND a.entity_code = 'SLEEK_SG'
        AND a.staff_grouping IN ('{staff_grouping}')
    ) a
  ) b
) c
LEFT JOIN `bigquery-production-309309.sleek_subscription.all_subscriptions` eot
  ON c.companyid = eot.company
  AND eot.type = 'corporate_secretary'
  AND eot.original_status NOT IN ('discontinued', 'inactive')
  AND eot.company_status NOT IN ('churn', 'archived', 'striked_off')
  AND eot.internalname = 'extension_of_time_agm';
"""

