import os
from pyflink.table import EnvironmentSettings, TableEnvironment

def main():
    # 1. 환경 설정
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = TableEnvironment.create(settings)

    # 2. 입력 테이블 (KDS에서 데이터 읽기)
    t_env.execute_sql(f"""
        CREATE TABLE stock_input (
            ticker STRING,
            price DOUBLE,
            event_time TIMESTAMP(3),            
            WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'de-ai-17-an2-kds-stock-input',
            'aws.region' = 'ap-northeast-2',
            'scan.stream.initpos' = 'LATEST',
            'format' = 'json'
        )
    """)

    # 3. 출력 테이블 (결과를 보낼 KDS 또는 Firehose용 스트림)
    t_env.execute_sql("""
        CREATE TABLE stock_output (
            ticker STRING,
            avg_price DOUBLE,
            window_time TIMESTAMP(3)
        ) WITH (
            'connector' = 'kinesis',
            'stream' = 'de-ai-17-an2-kds-stock-output',
            'aws.region' = 'ap-northeast-2',
            'format' = 'json'
        )
    """)

    # 4. 연산 및 전송
    t_env.execute_sql("""
        INSERT INTO stock_output
        SELECT 
            ticker, 
            AVG(price) as avg_price, 
            TUMBLE_END(event_time, INTERVAL '10' SECOND) as window_time
        FROM stock_input
        GROUP BY TUMBLE(event_time, INTERVAL '10' SECOND), ticker
    """).wait()

if __name__ == '__main__':
    main()