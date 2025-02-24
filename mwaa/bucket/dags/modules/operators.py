from airflow.providers.amazon.aws.operators.lambda_function import LambdaHook
from airflow.providers.amazon.aws.operators.lambda_function import (
    LambdaInvokeFunctionOperator as BaseLambdaInvokeFunctionOperator,
)
from botocore.config import Config


class LambdaInvokeFunctionOperator(BaseLambdaInvokeFunctionOperator):
    """
    기본 설정을 수정하기 위해 기존 LambdaInvokeFunctionOperator를 상속받은 클래스입니다.
    """
    def execute(self, context):
        """
        LambdaHook의 connect_timeout, read_timeout, tcp_keepalive 설정을 변경합니다.
        """
        self.hook = LambdaHook(
            aws_conn_id=self.aws_conn_id,
            config=Config(
                connect_timeout=60, # 연결 시간 초과를 60초로 설정
                read_timeout=1000, # AWS Lambda의 최대 제한 시간인 15분을 고려하여 1000으로 설정
                tcp_keepalive=True, # 주기적으로 TCP 연결 상태를 확인
            ),
        )

        return super().execute(context)
