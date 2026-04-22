'''
- 주가 로그 생성기, boto3를 이용하여 직접 연계
- 로그 -> Kinesis로 전달
- 필요 패키지
    pip install python-dotenv
'''
# 1. 모듈가져오기
import time
import random
import json
from datetime import datetime
import boto3
from dotenv import load_dotenv
import os

# 2. 환경변수 세팅
load_dotenv() # .env 내용을 읽어서 환경 변수로 설정
ACCESS_KEY = os.getenv("ACCESS_KEY")
SECRET_KEY = os.getenv("SECRET_KEY")
REGION     = 'ap-northeast-2'

print( ACCESS_KEY, SECRET_KEY )

# 3. 클라이언트 생성
def get_client( service_name='firehose', is_in_aws=True ):
    if not is_in_aws:
        #    AWS 외부에서 진행
        session   = boto3.Session(
            aws_access_key_id     = ACCESS_KEY,
            aws_secret_access_key = SECRET_KEY,
            region_name           = REGION
        )
        return session.client(service_name)    
    #   AWS 내부에서 진행
    return boto3.client(service_name, region_name = REGION)

kinesis = get_client('kinesis', False)
print( kinesis )

# 4. 데이터 제너레이터 함수 구성
def gen_stock_data():
    ticker = ['NVDA', 'GOOGL', 'AAPL', 'TSLA', 'AMZN', 'MSFT']
    # 종목별 특정 시간(기간)동안 평균가 연산 => s3 전달 목표
    return {
        "event_time": datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3],
        "ticker": random.choice(ticker), # 체크
        "price": round( random.uniform(100, 1000), 2), # 체크
        "volume": random.randint(1, 100),
        "trade_id": random.randint(100000, 9999999)
    }

# 5. 데이터 전송
print('stock 거래 데이터 전송 시작...')
try:
    while True:        
        data = gen_stock_data()                
        kinesis.put_record(
            # TODO:Flink 스트림 이름 수정
            StreamName = "de-ai-17-an2-kds-stock-input",
            Data = json.dumps( data ),
            PartitionKey = data['ticker']
        )        
        print( f"전송: {data}")        
        time.sleep(0.5)
except Exception as e:
    print('중단 ', e)