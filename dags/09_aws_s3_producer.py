'''
- 목표
    - 데이터 생산(etl등 통해서) -> CSV -> s3 업로드(PUSH) 처리
    - 배치 작업( 특정 시간대에 스케줄링하여 일괄 처리 ) -> airflow 목표
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import logging
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 2. 환경변수
# 버킷명 : {계정}-{root계정아이디}-{리전} <- 서비스명 누락 => 차후 리소스명 네이밍 컨벤션 체크
BUCKET_NAME = "de-ai-17-827913617635-ap-northeast-2-an"
# 업로드할 파일명
FILE_NAME   = 'sensor_data.csv'
# 버킷내에 특정 폴더 위치에 생성 -> KEY 지정 -> 버킷/income/xx.csv
S3_KEY      = f'income/{FILE_NAME}'
# 업로드할 로컬 파일의 위치 (컨테이너->리눅스 기반)
LOCAL_PATH  = f'/opt/airflow/dags/data/{FILE_NAME}'

# 3. DAG 정의
with DAG(
    dag_id      = "09_aws_s3_producer", 
    description = "aws 연동, s3 업로드",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },
    schedule_interval = None, # 스케줄 x -> 트리거 작동으로 실행
    start_date  = datetime(2026,2,25),     
    catchup     = False,
    tags        = ['aws', 's3', 'producer'],
) as dag:
    # 4. 오퍼레이터를 통한 task 정의
    task_create_dummy_data_csv = BashOperator(
        task_id = "task_create_dummy_data_csv",
        bash_command = f'echo "id,timestamp,value\n1,$(date),100\n2,$(date),500" > {LOCAL_PATH}'
    )
    task_upload_to_s3 = LocalFilesystemToS3Operator(
        task_id = "task_upload_to_s3",
        filename = LOCAL_PATH,  
        dest_key = S3_KEY,      # 버킷내 특저 위치
        dest_bucket = BUCKET_NAME,
        aws_conn_id = 'aws_default',
        replace  = True # 동일 파일이 있으면 덮는다 -> 그 순간 파일은 1개만 유일
    )
    # 5. 의존성, injection
    task_create_dummy_data_csv >> task_upload_to_s3
    pass
