'''
DAG에서 opensearch 검색 -> 데이터 획득
'''
# 1. 모듈 가져오기
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from opensearchpy import OpenSearch
import pendulum # 서울 시간대 간편하게 설정
from airflow.models import Variable
import pandas as pd

# 2. 환경변수
# HOST, AUTH, 인덱스(상황에 따라 별도 구성가능함)->검색어/패턴으로구성/고정등
HOST = Variable.get("HOST")
AUTH = (Variable.get("AUTH_NAME"), Variable.get("AUTH_PW"))
index_name = 'factory-45-sensor-v1' # 검색어 -> 인덱스 정보

# 4.1 opensearch를 통해 검색 후 결과 획득 콜백함수 ( _searching_proc )
def _searching_proc(**kwargs):
    # 4-1.1 클라이언트 연결
    client = OpenSearch(
        hosts         = [{"host": HOST, "port": 443}], # https -> 443
        http_auth     = AUTH,
        http_compress = True,
        use_ssl       = True,
        verify_certs  = True,
        ssl_assert_hostname = False,
        ssl_show_warn = False
    )

    # 4-1-2 opensearch용 쿼리 구성
    #       Query DSL : JSON으로 작성하여 SQL에 대응하는 개념\
    #       https://docs.opensearch.org/latest/query-dsl/
    # 검색엔진에 해당 인덱스로 검색 실시 -> 요청시간 기준 10분전부터 가져온다, 최대 1000개 
    query = {
        "size": 1000,
        "query": {
            "range":{
                "timestamp":{
                    "gte":"now-120m"  # greater then or equal (>=), now-10m:현재부터 10분전
                }
            }
        }
    }
    # 4-1-3 검색 요청
    # 인덱스 정보 + 상세 조건
    response = client.search(index=index_name, body=query)
    print( '검색결과', response )
    hits = response['hits']['hits']

    # 4-1-4 나온 결과 체크, 필요시 전처리등
    if not hits:
        print('조회 결과 없음')
        return
    else:
        print('조회 결과 수', len(hits) )

    # 4-1-5 분석 -> 요구사항(오븐별 평균 온도, 최대 진동등 계산), 이상탐지(허용범위 이상인 경우)
    # 분석이 가능한 형태의 자료구조 변형(pandas or pyspark등 활용-데이터체급에 따라 적용)
    data = [ hit['_source'] for hit in hits ] # 원(raw) data 획득 -> dict 형태
    # data => [ {}, {}, ..., {} ]
    df   = pd.DataFrame(data)
    print( df.sample(1) ) # 샘플 1개 출력

    # 요구사항 => 그릅화(groupby or 피벗테이블,..)하여 처리
    analysis = df.groupby('oven_id').agg({
        "temperature":"mean", # 평균 온도
        "vibration"  :"max",  # 최대 진동값
        "status"     :"count",# 총 로그수 
    }).rename(columns={
        'status'      : 'log_count',
        "temperature" : 'temp_mean',
        "vibration"   : 'vib_max'
    })
    print( "최근 120분간 오븐별 평균온도, 최대 진동, 발생로그수" )
    print( analysis )

    # 이상치 탐지 => 오븐 온도가 230도 이상인 데이터만 필터링 => 블리언(조건식) 인덱싱 사용
    # df => 2차원/매트릭스, series => 1차원/백터, 0차원 => 값/스칼라
    out_of_data = df[ df['temperature'] >= 230 ]
    if len(out_of_data):
        print('이상 온도 감지 건수', len(out_of_data) )
        # 차후 오븐별(장비별) 발생 건수 추출가능 -> 리뷰대 검토

    # 4-1-6 분석 결과 출력
    pass

# 3. DAG 정의
with DAG(
    dag_id      = "13_opensearch_read", 
    description = "검색엔진에 대해 질의 후 결과 획득",
    default_args= {
        'owner'             : 'de_2team_manager',        
        'retries'           : 1,
        'retry_delay'       : timedelta(minutes=1)
    },    
    schedule_interval = '*/10 * * * *',
    start_date  = pendulum.datetime(2026,1,1,tz="Asia/Seoul"), # 서울 시간대 1월 1일
    catchup     = False,
    tags        = ['aws', 'opensearch'],
) as dag:
    # 4. task 정의
    task = PythonOperator(
        task_id = 'searching_proc',
        python_callable = _searching_proc
    )
    # 5. 의존성
    #task