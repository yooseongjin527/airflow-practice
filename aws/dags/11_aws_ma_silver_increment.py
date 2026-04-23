'''
- ma 에서 silver 단계 처리
- silver 레벨에서 데이터를 계속 증분한다
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.amazon.aws.operators.athena import AthenaOperator

# 2. 환경변수
DATABASE_BRONZE = 'de_ai_17_ma_bronze_db' 
DATABASE_SILVER = 'de_ai_17_ma_silver_db'
# 데이터 저장용 -> 실데이터
SILVER_S3_PATH  = 's3://de-ai-17-827913617635-ap-northeast-2-an/medallion/silver/'
# 쿼리 히스토리등 저장용 -> 메타
ATHENA_RESULTS  = 's3://de-ai-17-827913617635-ap-northeast-2-an/athena-results/'
SILVER_TBL_NAME = 'sales_silver_increment_tbl'

# 3. DAG 정의
with DAG(
    dag_id      = "11_medallion_bronze_to_silver_increment", 
    description = "athena 증분 작업",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = '10 * * * *', # 매시 10분에 진행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 'medallion', 'silver', 'athena', 'increment'],
) as dag:
    # [TASK 1] Silver 테이블이 없을 경우에만 생성 (Schema 정의)
    # CTAS가 아니므로 데이터 없이 구조만 먼저 만듬
    create_silver_table = AthenaOperator(
        task_id='create_silver_table_if_not_exists',
        query="""
            CREATE EXTERNAL TABLE IF NOT EXISTS {{ params.database_silver }}.{{ params.tbl_nm }} (
                event_id string,
                event_timestamp timestamp,
                user_id string,
                item_id string,
                price int,
                qty int,
                total_price int,
                store_id string,
                source_ip string,
                user_agent string
            )
            PARTITIONED BY (dt string, hr string)
            STORED AS PARQUET
            LOCATION '{{ params.silver_path }}'
            TBLPROPERTIES ('parquet.compress'='SNAPPY');
        """,
        params={
            'database_silver': DATABASE_SILVER,
            'silver_path': SILVER_S3_PATH,
            'tbl_nm':SILVER_TBL_NAME
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS
    )

    # [TASK 2] 특정 시간대의 데이터를 추출하여 Silver 테이블에 삽입 (Incremental Load)
    # execution_date를 활용해 딱 해당 시간의 데이터만 골라냄
    insert_silver_data = AthenaOperator(
        task_id='insert_bronze_to_silver',
        query="""
            INSERT INTO {{ params.database_silver }}.{{ params.tbl_nm }}
            SELECT
                event_id,
                event_time as event_timestamp,
                data.user_id,
                data.item_id,
                data.price,
                data.qty,
                (data.price * data.qty) as total_price,
                data.store_id,
                source_ip,
                user_agent,
                -- 파티션 컬럼
                CAST(year || '-' || month || '-' || day AS VARCHAR) as dt,
                hour as hr
            FROM {{ params.database_bronze }}.raw_bronze_tbl
            WHERE year = '{{ execution_date.format("YYYY") }}'
            AND month = '{{ execution_date.format("MM") }}'
            AND day = '{{ execution_date.format("DD") }}'
            AND hour = '{{ execution_date.format("HH") }}';
        """,
        params={
            'database_bronze': DATABASE_BRONZE,
            'database_silver': DATABASE_SILVER,
            'tbl_nm':SILVER_TBL_NAME            
        },
        database=DATABASE_SILVER,
        output_location=ATHENA_RESULTS
    )

    # 태스크 순서 설정
    create_silver_table >> insert_silver_data