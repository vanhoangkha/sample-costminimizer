from ..utils.yaml_loader import import_yaml_file
import yaml
import logging

class ReportRequestFromSSM:

    def __init__(self, parameters_prefix='pgai-'):
        from ..config.config import Config
        self.logger = logging.getLogger(__name__)

        self.appConfig = Config()
        self.ssm_s3_parameter = f'costminimizer-report-requests'
        self.s3_file_name = 'report_request.yaml'

    def get_ssm_client(self):
        return self.appConfig.auth_manager.aws_cow_account_boto_session.client('ssm', region_name=self.appConfig.default_selected_region)
    
    def get_s3_client(self):
        return self.appConfig.auth_manager.aws_cow_account_boto_session.client('s3', region_name=self.appConfig.default_selected_region)
    
    def get_report_request(self):
        ssm_client = self.get_ssm_client()
        s3_client = self.get_s3_client()
        try:
            response = ssm_client.get_parameter(Name=self.ssm_s3_parameter, WithDecryption=True)
            s3_bucket = response['Parameter']['Value']

            if self.appConfig.arguments_parsed.debug:
                self.appConfig.console.print(f'[blue]Report request SSM S3 bucket parameter: {self.ssm_s3_parameter}')
                self.appConfig.console.print(f'[blue]Report request bucket: {s3_bucket}')

            try:
                obj = s3_client.get_object(Bucket=s3_bucket, Key=self.s3_file_name)
            except Exception as e:
                msg = f"Error retrieving report request {self.s3_file_name} from S3: {e}"
                self.appConfig.logger.error(msg)
                self.appConfig.console.print(msg)

            #report_request = import_yaml_file(obj['Body'].read().decode('utf-8'))
            report_request = yaml.safe_load(obj['Body'].read().decode('utf-8'))
            if self.appConfig.arguments_parsed.debug:
                self.appConfig.console.print(f'[blue]Report request: {report_request}')
            else:
                self.appConfig.logger.info(f"Report request retrieved from SSM: {s3_bucket}")
                self.appConfig.console.print(f'[green]Report request retrieved from SSM: {s3_bucket}')
            return report_request
        except Exception as e:
            self.appConfig.logger.error(f"Error retrieving report request from SSM: {e}")
            raise
