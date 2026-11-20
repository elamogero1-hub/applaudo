from datetime import datetime, timezone
import os
from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook

PROJECT_ID = "gsw-analytics-sandbox"
EXTRACT = f"{PROJECT_ID}.zuora_extract.tickets_extract"
RUNS = f"{PROJECT_ID}.zuora_extract.tickets_run"
SOURCE_VIEW = "gsw-analytics-warehouse-prod.dev_zuora.ticketmaster_tickets"

# outbox: the Composer "data" folder is synced to gs://gsw-composer-gh/data/
# when the SFTP is ready, only deliver() changes (upload to /data/input instead of writing here)
OUTBOX = "/home/airflow/gcs/data/zuora_extract/outbox"
SEND_EMPTY = False          # pending with Zuora: empty CM file or no file


def bq(sql):
    hook = BigQueryHook(gcp_conn_id="google_cloud_default", use_legacy_sql=False)
    job = hook.insert_job(configuration={"query": {"query": sql, "useLegacySql": False}}, project_id=PROJECT_ID)
    return job.result()


def deliver(name, content):
    # write as .tmp and rename at the end, so a partial file is never picked up
    os.makedirs(OUTBOX, exist_ok=True)
    tmp = os.path.join(OUTBOX, name + ".tmp")
    with open(tmp, "w", newline="") as fh:
        fh.write(content)
    os.replace(tmp, os.path.join(OUTBOX, name))
    print(f"delivered {OUTBOX}/{name}")


with DAG(
    dag_id="zuora_tickets_test",
    description="Zuora tickets extract - test (RUN_ID + extract + INV/CM files to GCS outbox)",
    start_date=datetime(2026, 11, 1),
    schedule=None,                 # manual trigger only
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=True,
    tags=["zuora", "test"],
) as dag:

    @task
    def new_run_id():
        rid = list(bq(f"select coalesce(max(RUN_ID), 0) + 1 as id from `{RUNS}`"))[0].id
        bq(f"insert into `{RUNS}` (RUN_ID, FILE_TYPE, STATUS, CREATED_AT) "
           f"values ({rid}, 'RUN', 'RUNNING', current_timestamp())")
        print(f"RUN_ID = {rid}")
        return rid

    @task
    def extract(rid):
        bq(f"""
        insert into `{EXTRACT}` (
           RUN_ID
          ,LINE_TYPE
          ,SOURCE_SYSTEM
          ,CUSTOMER_NAME
          ,CUSTOMER_NUMBER
          ,EVENT_ID
          ,PRICE_CODE_GROUP
          ,PARENT_PRICE_CODE
          ,EVENT_DATE
          ,FULL_PRICE_CODE
          ,EVENT_NAME_LONG
          ,SUBSIDIARY
          ,SUBSIDIARY_INTERNAL_ID
          ,SALES_ORDER_NUM
          ,SALES_ORDER_LINE_NUM
          ,SALES_ORDER_DATE
          ,SALES_ORDER_LINE_ID
          ,INV_NUM
          ,INV_LINE_NUM
          ,INVOICE_LINE_ID
          ,INVOICE_DATE
          ,ORIG_INV_LINE_ID
          ,ORDERED_QTY
          ,INVOICE_QTY
          ,REVENUE_START_DATE
          ,REVENUE_END_DATE
          ,LINE_ITEM_NUM
          ,POB_IDENTIFIER
          ,EVENT_NAME
          ,EXT_LIST_PRICE
          ,EXT_SELL_PRICE
          ,TRANSACTION_CURRENCY
          ,FUNCTIONAL_CURRENCY
          ,FUNCTIONAL_EX_RATE
          ,GLOBAL_EX_RATE
          ,DEFERRED_SEGMENTS
          ,REVENUE_SEGMENTS
          ,CV_ELIGIBLE_FLAG
          ,FV_ELIGIBLE_FLAG
          ,FOOD_AND_BEVERAGES
          ,PARKING
          ,FACILITIES_FEE
          ,RIGHT_TO_BILL_FLAG
          ,SUBSCRIPTION_NUMBER
          ,TERM
          ,PRODUCT_CLASS
          ,PRODUCT_CATEGORY
          ,PRODUCT_LINE
          ,SF_TRANSPORTATION_FEE
          ,ADMISSION_FEE
          ,AMENITIES
          ,SEASON_START_DATE
          ,LINE_STATUS
          ,SENT_RUN_ID
          ,SENT_AT
        )
        select
           {rid}
          ,cast(f.Line_Type as STRING)
          ,cast(f.Source_System as STRING)
          ,cast(f.Customer_Name as STRING)
          ,cast(f.Customer_Number as INT64)
          ,cast(f.EventID as INT64)
          ,cast(f.Price_Code_Group as STRING)
          ,cast(f.Parent_Price_Code as STRING)
          ,cast(f.EventDate as DATE)
          ,cast(f.Full_Price_code as STRING)
          ,cast(f.Event_Name_Long as STRING)
          ,cast(f.Subsidiary as STRING)
          ,cast(f.SubsidiaryInternalID as INT64)
          ,cast(f.Sales_Order_Num as STRING)
          ,cast(f.Sales_Order_Line_Num as STRING)
          ,cast(f.Sales_Order_Date as DATE)
          ,cast(f.SalesOrderLineID as STRING)
          ,cast(f.INV_NUM as STRING)
          ,cast(f.INV_LINE_NUM as STRING)
          ,cast(f.Invoice_Line_ID as STRING)
          ,cast(f.Invoice_Date as DATE)
          ,cast(f.ORIG_INV_LINE_ID as STRING)
          ,cast(f.OrderedQty as INT64)
          ,cast(f.InvoiceQty as INT64)
          ,cast(f.Revenue_Start_Date as DATE)
          ,cast(f.Revenue_End_Date as DATE)
          ,cast(f.Line_Item_Num as STRING)
          ,cast(f.POBIdentifier as STRING)
          ,cast(f.EventName as STRING)
          ,cast(f.Ext_List_Price as FLOAT64)
          ,cast(f.Ext_Sell_Price as FLOAT64)
          ,cast(f.Transaction_Currency as STRING)
          ,cast(f.Functional_Currency as STRING)
          ,cast(f.Functional_Ex_Rate as FLOAT64)
          ,cast(f.Global_Ex_Rate as FLOAT64)
          ,cast(f.DeferredSegments as STRING)
          ,cast(f.RevenueSegments as STRING)
          ,cast(f.CV_ELIGIBLE_FLAG as STRING)
          ,cast(f.FV_ELIGIBLE_FLAG as STRING)
          ,cast(f.Food_and_Beverages as STRING)
          ,cast(f.Parking as STRING)
          ,cast(f.Facilities_fee as FLOAT64)
          ,cast(f.RightToBillFlag as STRING)
          ,cast(f.Subscription_Number as STRING)
          ,cast(f.Term as STRING)
          ,cast(f.Product_Class as STRING)
          ,cast(f.Product_Category as STRING)
          ,cast(f.Product_line as STRING)
          ,cast(f.SF_Transportation_Fee as STRING)
          ,cast(f.Admission_Fee as FLOAT64)
          ,cast(f.Amenities as STRING)
          ,cast(f.Season_start_date as DATE)
          -- future season (season start after the current season start of the entity): hold until F&B / parking rates exist
          ,case
             when f.Season_start_date > case
                 when f.Subsidiary in ('Golden State Warriors, LLC', 'SC Warriors LLC')
                   then date(extract(year from current_date) - if(extract(month from current_date) >= 10, 0, 1), 10, 1)
                 when f.Subsidiary = 'Golden State Valkyries LLC'
                   then date(extract(year from current_date) - if(extract(month from current_date) >= 4, 0, 1), 4, 1)
               end then 'HOLD'
             else 'READY'
           end
          ,cast(null as int64)
          ,cast(null as timestamp)
        from `{SOURCE_VIEW}` f
        where f.Invoice_Line_ID is not null
          -- only lines not extracted before (price in the key so a re-priced line is not blocked)
          and not exists (
            select 1 from `{EXTRACT}` x
            where x.INVOICE_LINE_ID = f.Invoice_Line_ID
              and x.EXT_SELL_PRICE = cast(f.Ext_Sell_Price as float64)
          )
        """)
        for r in bq(f"select LINE_TYPE, LINE_STATUS, count(*) n from `{EXTRACT}` "
                    f"where RUN_ID = {rid} group by 1, 2 order by 1, 2"):
            print(r.LINE_TYPE, r.LINE_STATUS, r.n)
        return rid

    @task
    def release_holds(rid):
        # TODO: rates source not defined yet (table/file, granularity)
        # update `{EXTRACT}` x set LINE_STATUS = 'READY', FOOD_AND_BEVERAGES = r..., PARKING = r...
        # from <rates_table> r where x.LINE_STATUS = 'HOLD' and <join by season / price code>
        return rid

    @task
    def send_file(rid, line_type):
        where = f"LINE_STATUS = 'READY' and LINE_TYPE = '{line_type}'"
        # file header uses the Zuora template names
        df = bq(f"""
        select
           LINE_TYPE as Line_Type
          ,SOURCE_SYSTEM as Source_System
          ,CUSTOMER_NAME as Customer_Name
          ,CUSTOMER_NUMBER as Customer_Number
          ,EVENT_ID as EventID
          ,PRICE_CODE_GROUP as Price_Code_Group
          ,PARENT_PRICE_CODE as Parent_Price_Code
          ,EVENT_DATE as EventDate
          ,FULL_PRICE_CODE as Full_Price_code
          ,EVENT_NAME_LONG as Event_Name_Long
          ,SUBSIDIARY as Subsidiary
          ,SUBSIDIARY_INTERNAL_ID as SubsidiaryInternalID
          ,SALES_ORDER_NUM as Sales_Order_Num
          ,SALES_ORDER_LINE_NUM as Sales_Order_Line_Num
          ,SALES_ORDER_DATE as Sales_Order_Date
          ,SALES_ORDER_LINE_ID as SalesOrderLineID
          ,INV_NUM as INV_NUM
          ,INV_LINE_NUM as INV_LINE_NUM
          ,INVOICE_LINE_ID as Invoice_Line_ID
          ,INVOICE_DATE as Invoice_Date
          ,ORIG_INV_LINE_ID as ORIG_INV_LINE_ID
          ,ORDERED_QTY as OrderedQty
          ,INVOICE_QTY as InvoiceQty
          ,REVENUE_START_DATE as Revenue_Start_Date
          ,REVENUE_END_DATE as Revenue_End_Date
          ,LINE_ITEM_NUM as Line_Item_Num
          ,POB_IDENTIFIER as POBIdentifier
          ,EVENT_NAME as EventName
          ,EXT_LIST_PRICE as Ext_List_Price
          ,EXT_SELL_PRICE as Ext_Sell_Price
          ,TRANSACTION_CURRENCY as Transaction_Currency
          ,FUNCTIONAL_CURRENCY as Functional_Currency
          ,FUNCTIONAL_EX_RATE as Functional_Ex_Rate
          ,GLOBAL_EX_RATE as Global_Ex_Rate
          ,DEFERRED_SEGMENTS as DeferredSegments
          ,REVENUE_SEGMENTS as RevenueSegments
          ,CV_ELIGIBLE_FLAG as CV_ELIGIBLE_FLAG
          ,FV_ELIGIBLE_FLAG as FV_ELIGIBLE_FLAG
          ,FOOD_AND_BEVERAGES as Food_and_Beverages
          ,PARKING as Parking
          ,FACILITIES_FEE as Facilities_fee
          ,RIGHT_TO_BILL_FLAG as RightToBillFlag
          ,SUBSCRIPTION_NUMBER as Subscription_Number
          ,TERM as Term
          ,PRODUCT_CLASS as Product_Class
          ,PRODUCT_CATEGORY as Product_Category
          ,PRODUCT_LINE as Product_line
          ,SF_TRANSPORTATION_FEE as SF_Transportation_Fee
          ,ADMISSION_FEE as Admission_Fee
          ,AMENITIES as Amenities
          ,SEASON_START_DATE as Season_start_date
        from `{EXTRACT}`
        where {where}
        order by INVOICE_LINE_ID
        """).to_dataframe(create_bqstorage_client=False)   # no Storage API permission: use the REST download
        print(f"{line_type} lines READY: {len(df)}")
        if df.empty and not SEND_EMPTY:
            return rid

        name = f"GSW_TICKETS_{line_type}_{rid:05d}_{datetime.now(timezone.utc):%Y%m%d}.csv"
        deliver(name, df.to_csv(index=False))

        bq(f"update `{EXTRACT}` set LINE_STATUS = 'SENT', SENT_RUN_ID = {rid}, "
           f"SENT_AT = current_timestamp() where {where}")
        bq(f"insert into `{RUNS}` (RUN_ID, FILE_TYPE, STATUS, FILE_NAME, ROW_COUNT, CREATED_AT) "
           f"values ({rid}, '{line_type}', 'SENT', '{name}', {len(df)}, current_timestamp())")
        return rid

    @task
    def close_run(rid):
        bq(f"""update `{RUNS}` set STATUS = 'DONE',
               ROW_COUNT = (select count(*) from `{EXTRACT}` where RUN_ID = {rid})
               where RUN_ID = {rid} and FILE_TYPE = 'RUN'""")

    rid = release_holds(extract(new_run_id()))
    inv = send_file.override(task_id="send_inv")(rid, "INV")
    cm = send_file.override(task_id="send_cm")(inv, "CM")     # CM always after INV
    close_run(cm)
