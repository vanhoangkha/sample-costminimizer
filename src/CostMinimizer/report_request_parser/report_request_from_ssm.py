from ..utils.yaml_loader import import_yaml_file
import yaml

class ReportRequestFromSSM:

    def __init__(self, parameters_prefix='pgai-'):
        from ..config.config import Config

        self.appConfig = Config()
        self.ssm_s3_parameter = f'{parameters_prefix}report-requests'
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
            obj = s3_client.get_object(Bucket=s3_bucket, Key=self.s3_file_name)
            #report_request = import_yaml_file(obj['Body'].read().decode('utf-8'))
            report_request = yaml.safe_load(obj['Body'].read().decode('utf-8'))
            return report_request
        except Exception as e:
            self.appConfig.logger.error(f"Error retrieving report request from SSM: {e}")
            raise
