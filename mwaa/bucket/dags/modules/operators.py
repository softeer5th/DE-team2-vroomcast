from botocore.config import Config
from airflow.providers.amazon.aws.operators.lambda_function import \
    LambdaInvokeFunctionOperator as BaseLambdaInvokeFunctionOperator, LambdaHook

class LambdaInvokeFunctionOperator(BaseLambdaInvokeFunctionOperator):
    def execute(self, context):
        self.hook = LambdaHook(aws_conn_id=self.aws_conn_id, config=Config(
            connect_timeout=60,
            read_timeout=1000,
            tcp_keepalive=True,
        ))

        return super().execute(context)