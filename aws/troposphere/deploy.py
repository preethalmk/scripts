from stacker.blueprints.base import Blueprint
from troposphere import ImportValue, Sub, Template, Ref, GetAtt
from troposphere.codedeploy import DeploymentGroup, DeploymentStyle, TriggerConfig
from troposphere.iam import Role, Policy
from troposphere.sqs import Queue, RedrivePolicy, QueuePolicy
from troposphere.serverless import Function, VPCConfig, Environment
from troposphere.logs import LogGroup
from troposphere.stepfunctions import StateMachine

from pnmac import (
    DEFAULT_LAMBDA_EXEC_ROLE,
    DEFAULT_SFN_EXEC_ROLE # confirm the exact variable
    
)

import logging
logger = logging.getLogger(__name__)

class PatchedBlueprint(Blueprint):
    def set_template_description(self, description):
        self.template.set_description(description)

class PennyMac(PatchedBlueprint):
    VARIABLES = {
        # Common Lambda Variables
        'Env': {
            'type': str,
            'description': 'Short Environment Name'
        },
        'CodeDeployBucket': {
            'type': str,
            'description': 'CodeDeploy Bucket Name'
        },
        'CodeDeployAppName': {
            'type': str,
            'description': 'CodeDeploy Application Name'
        },
        'LambdaSecurityGroup': {
            'type': str,
            'description': 'Lambda Security Group',
            'default': 'sg-1234567890'
        },
        'LambdaLogRetentionDays': {
            'type': int,
            'description': 'Lambda log retention in days'
        },
        'KMSKeyArn': {
            'type': str,
            'description': 'Shared KMS Key ARN',
            'default': 'arn:aws:kms:us-west-2:038736731981:key/038736731981'
        },
        'DBSecretArn': {
            'type': str,
            'description': 'DB Secret ARN',
            'default': 'arn:aws:secretsmanager:us-west-2:038736731981:secret:038736731981'
        },
        'ASPNETCORE_ENVIRONMENT': {
            'type': str,
            'description': 'ASPNETCORE_ENVIRONMENT'
        },

        # CustomerNotifications Variables
        'LambdaName': {
            'type': str,
            'description': 'Name of the Lambda'
        },
        'LambdaMemorySize': {
            'type': int,
            'description': 'Lambda Memory Size',
            'default': 1024
        },
        'LambdaTimeout': {
            'type': int,
            'description': 'Lambda Timeout',
            'default': 900
        },
        'LambdaReservedConcurrentExecutions': {
            'type': int,
            'description': 'Lambda Reserved Concurrent Executions',
            'default': 50
        },
        'QueueName': {
            'type': str,
            'description': 'Queue Name'
        },
        'QueueMessageRetentionPeriod': {
            'type': int,
            'description': 'Queue Message Retention Period',
            'default': 1209600
        },
        'QueueVisibilityTimeout':{
            'type': int,
            'description': 'Queue Visibility Timeout',
            'default': 1800
        },
        'QueueReceiveMessageWaitTimeSeconds': {
            'type': int,
            'description': 'Queue Receive Message Wait Time Seconds',
            'default': 20
        },
        'QueueMaxReceiveCount': {
            'type': int,
            'description': 'Queue Max Receive Count',
            'default': 10
        },
        'LambdaEventBatchSize':{
            'type': int,
            'description': 'Lambda Event Queue Batch Size',
            'default': 200
        },
        'LambdaEventMaxBatchWindow':{
            'type': int,
            'description': 'Lambda Event Queue Maximum Batching Window',
            'default': 30
        }
    }

    template = Template()

    def initialize_template(self):
        self.config = self.get_variables()
        
    def Correspondence_sfn_defnition(self)
        return {
                  "Comment": "Sate Machine to parallelly process EmailNotification to PDF ",
                  "StartAt": "GeneratePDFs",
                  "States": {
                    "GeneratePDFs": {
                      "Type": "Map",
                      "InputPath": "$",
                      "ItemsPath": "$.SendLogList",
                      "ItemProcessor": {
                        "ProcessorConfig": {
                          "Mode": "INLINE"
                        },
                        "StartAt": "Generate PDF",
                        "States": {
                          "Generate PDF": {
                            "Type": "Task",
                            "Resource": "arn:aws:states:::lambda:invoke",
                            "Parameters": {
                              "Payload.$": "$",
                              "FunctionName":  "${functionName}"
                            },
                            "Retry": [
                              {
                                "ErrorEquals": [
                                  "States.ALL"
                                ],
                                "IntervalSeconds": 1,
                                "BackoffRate": 2,
                                "MaxAttempts": 0
                              }
                            ],
                            "Catch": [
                              {
                                "ErrorEquals": [
                                  "States.ALL"
                                ],
                                "Comment": "Catch lambda Error and Continue",
                                "Next": "Handle Exception Data",
                                "ResultPath": "$.ErrorDetails"
                              }
                            ],
                            "Next": "Pass Success Message",
                            "ResultPath": "$.TaskResult"
                          },
                          "Handle Exception Data": {
                            "Type": "Pass",
                            "Parameters": {
                              "SQSMessageReceiptHandle.$": "$.SQSMessageReceiptHandle",
                              "Id.$": "$.Id",
                              "LoanId.$": "$.LoanId",
                              "EmailNotificationId.$": "$.EmailNotificationId",
                              "DocTypeId.$": "$.DocTypeId",
                              "SourceLID.$": "$.SourceLID",
                              "ScanDate.$": "$.ScanDate",
                              "DocumentURL.$": "$.DocumentURL",
                              "ErrorDetails.$": "$.ErrorDetails"
                            },
                            "End": True
                          },
                          "Pass Success Message": {
                            "Type": "Pass",
                            "End": True,
                            "Parameters": {
                              "SQSMessageReceiptHandle.$": "$.SQSMessageReceiptHandle",
                              "Id.$": "$.Id",
                              "LoanId.$": "$.LoanId",
                              "EmailNotificationId.$": "$.EmailNotificationId",
                              "DocTypeId.$": "$.DocTypeId",
                              "SourceLID.$": "$.SourceLID",
                              "ScanDate.$": "$.ScanDate",
                              "DocumentURL.$": "$.DocumentURL",
                              "StatusId.$": "$.TaskResult.Payload.StatusId",
                              "ErrorHtmlString.$": "$.TaskResult.Payload.ErrorHtmlString"
                            }
                          }
                        }
                      },
                      "End": True
                    }
                  }
                }
        

    def CustomerNotificationsConvertToPDF(self):
        function_logical_id = self.config['LambdaName'] + 'Lambda'
        function_name = '-'.join(['SVT',self.config['LambdaName'],self.config['Env']])
        
        # Correspondence lambda parameters
        CorrespondenceLambdaName ='CorrespondenceConvertToPDF'
        Correspondencefunction_logical_id = CorrespondenceLambdaName + 'Lambda'
        Correspondencefunction_name = '-'.join(['SVT',CorrespondenceLambdaName,self.config['Env']])
        
        #Correspondence SFN parameters
        CorrespondenceSfnName='Correspondence-CutomerNotificationsConvertToPDF'
        sfn_logical_id = CorrespondenceSfnName + 'sfn'
        sfn_name = '-'.join(['SVT',CorrespondenceSfnName,self.config['Env']])
 
        # create queue resources
        self.template.add_resource(Queue(
            'Queue',
            QueueName = '-'.join(['SVT',self.config['QueueName'],self.config['Env']]),
            MessageRetentionPeriod = self.config['QueueMessageRetentionPeriod'],
            VisibilityTimeout = self.config['QueueVisibilityTimeout'],
            ReceiveMessageWaitTimeSeconds = self.config['QueueReceiveMessageWaitTimeSeconds'],
            RedrivePolicy = RedrivePolicy (
                deadLetterTargetArn = GetAtt('QueueDLQ', 'Arn'),
                maxReceiveCount = self.config['QueueMaxReceiveCount']
            )
        ))
        
        self.template.add_resource(Queue(
            'QueueDLQ',
            QueueName = '-'.join(['SVT',self.config['QueueName'],'DeadLetter',self.config['Env']]),
            MessageRetentionPeriod = self.config['QueueMessageRetentionPeriod']
        ))

        self.template.add_resource(QueuePolicy(
            'QueueSQSPolicy',
            Queues=[
                Ref("Queue"),
            ],
            PolicyDocument={
                "Version": "2012-10-17",
                "Id": "QueueSQSPolicy",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": "*",
                        "Action": [
                            "sqs:DeleteMessage",
                            "sqs:ListQueues",
                            "sqs:PurgeQueue",
                            "sqs:ReceiveMessage",
                            "sqs:SendMessage"
                        ],
                        "Resource": {
                            "Fn::Sub": "arn:aws:sqs:${AWS::Region}:${AWS::AccountId}:SVT-CustomerNotifications-ConvertToPDF-" + self.config['Env']
                        },
                        "Condition": {
                            "ArnLike": {
                                "aws:SourceArn": [
                                    {
                                        "Fn::Sub": "arn:aws:iam::${AWS::AccountId}:role/svt-sql-shared-" + self.config['Env'] + "-SSISRole"
                                    }
                                ]
                            }
                        }
                    }
                ]
            }
        ))

        # create code deploy deployment group
        self.template.add_resource(DeploymentGroup(
            function_logical_id + 'CodeDeployGroup',
            ApplicationName = self.config['CodeDeployAppName'],
            DeploymentGroupName = function_name,
            DeploymentConfigName='CodeDeployDefault.LambdaAllAtOnce',
            DeploymentStyle = DeploymentStyle(
                DeploymentType='BLUE_GREEN',
                DeploymentOption='WITH_TRAFFIC_CONTROL'
            ),
            ServiceRoleArn = Sub(
                'arn:aws:iam::${AWS::AccountId}:role/CodeDeployRoleForLambda'),
            TriggerConfigurations=[
                TriggerConfig(
                    TriggerName='code-deploy-invoke-marker-lambda',
                    TriggerTargetArn='arn:aws:sns:us-west-2:038736731981:deployment-marker',
                    TriggerEvents=[
                        'DeploymentSuccess', 'DeploymentFailure']
                )
            ],
        ))

        # create lambda function
        self.template.add_resource(Role(
            function_logical_id + 'Role',
            Path='/',
            Policies=[
                Policy(
                    PolicyName="LambdaDefaultPolicy",
                    PolicyDocument = DEFAULT_LAMBDA_EXEC_ROLE
                ),
                Policy(
                    PolicyName="LambdaSecretPolicy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    'secretsmanager:GetSecretValue',
                                    'kms:Decrypt',
                                ],
                                "Resource": [
                                    self.config['KMSKeyArn'],
                                    self.config['DBSecretArn'],
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                ),
                Policy(
                    PolicyName="LambdaSfnPolicy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    'states:DescribeActivity',
                                    "states:DescribeExecution",                "states:ListExecutions",                "states:StartExecution",                "states:StartSyncExecution"
                                ],
                                "Resource": [Sub("arn:aws:states:${AWS::Region}:${AWS::Region}:activity:")+f"{sfn_name}"),
                                Sub("arn:aws:states:${AWS::Region}:${AWS::Region}:execution:")+f"{sfn_name}:"),
                                Sub("arn:aws:states:${AWS::Region}:${AWS::Region}:mapRun:")+f"{sfn_name}/:"),                
                                Sub("arn:aws:states:${AWS::Region}:${AWS::Region}:stateMachine:")+f"{sfn_name}"), 
                                Sub("arn:aws:states:${AWS::Region}:${AWS::Region}:express:")+f"{sfn_name}::*")
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                )
            ],
            ManagedPolicyArns=[
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSQSPolicy'),
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSNSPolicy'),
            ],
            AssumeRolePolicyDocument={
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Action': ['sts:AssumeRole'],
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': [
                                'lambda.amazonaws.com'
                            ]
                        }
                    }
                ]
            },
            ))
            
        # create Correspondence lambda function
        self.template.add_resource(Role(
            Correspondencefunction_logical_id + 'Role',
            Path='/',
            Policies=[
                Policy(
                    PolicyName="LambdaDefaultPolicy",
                    PolicyDocument = DEFAULT_LAMBDA_EXEC_ROLE
                ),
                Policy(
                    PolicyName="LambdaSecretPolicy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    'secretsmanager:GetSecretValue',
                                    'kms:Decrypt',
                                ],
                                "Resource": [
                                    self.config['KMSKeyArn'],
                                    self.config['DBSecretArn'],
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                ),
                Policy(
                    PolicyName="LambdaS3Policy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    "s3:ListBucket",
                                    "s3:PutObject",
                                    "s3:DeleteObject"
                                ],
                                "Resource": [
                                   "arn:aws:s3:::pnmac-pennedocs-bulkupload-dev/*",
                                    "arn:aws:s3:::pnmac-pennedocs-bulkupload-dev" # check self.config['CodeDeployBucket'] value to replace hardcoded value
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                )
            ],
            ManagedPolicyArns=[
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSQSPolicy'),
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSNSPolicy'),
            ],
            AssumeRolePolicyDocument={
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Action': ['sts:AssumeRole'],
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': [
                                'lambda.amazonaws.com'
                            ]
                        }
                    }
                ]
            },
            ))

        # create SFN role
        self.template.add_resource(Role(
            CorrespondenceSfnName + 'Role',
            Path='/',
            Policies=[
                Policy(
                    PolicyName="SfnDefaultPolicy",
                    PolicyDocument = DEFAULT_SFN_EXEC_ROLE
                ),
                Policy(
                    PolicyName="SfnSecretPolicy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    'secretsmanager:GetSecretValue',
                                    'kms:Decrypt',
                                ],
                                "Resource": [
                                    self.config['KMSKeyArn'],
                                    self.config['DBSecretArn'],
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                ),
                Policy(
                    PolicyName="LambdaInvokePolicy",
                    PolicyDocument = {
                        "Statement": [ 
                            {
                                "Action": [
                                    "lambda:InvokeAsync",
                                    "lambda:InvokeFunction"
                                ],
                                "Resource": [
                                   GetAtt(function_logical_id + 'Arn')
                                ],
                                "Effect": "Allow"
                            }
                        ]
                    }
                )
            ],
            ManagedPolicyArns=[
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSQSPolicy'),
                Sub('arn:aws:iam::${AWS::AccountId}:policy/SVTCoreSNSPolicy'),
            ],
            AssumeRolePolicyDocument={
                'Version': '2012-10-17',
                'Statement': [
                    {
                        'Action': ['sts:AssumeRole'],
                        'Effect': 'Allow',
                        'Principal': {
                            'Service': [
                                'lambda.amazonaws.com'
                            ]
                        }
                    }
                ]
            },
            ))



        environment_variables = {
            'NEW_RELIC_DISTRIBUTED_TRACING_ENABLED':True,
            'NEW_RELIC_SERVERLESS_MODE_ENABLED': True,
            'NEW_RELIC_ACCOUNT_ID': 399786,
            'NEW_RELIC_TRUSTED_ACCOUNT_KEY': 399786,
            'NEW_RELIC_USE_DT_WRAPPER': True,
            'NEW_RELIC_NO_CONFIG_FILE': True,
            'KMS_KEY_ARN': self.config['KMSKeyArn'],
            'DB_SECRET_ARN': self.config['DBSecretArn'],
            'ASPNETCORE_ENVIRONMENT': self.config['ASPNETCORE_ENVIRONMENT']
        }
        
        Correspondence_env_variables=environment_variables.copy()
        Correspondence_env_variables['SQS_CUSTOMER_NOTIFICATION_URL']=Sub( 'https://sqs.${AWS::Region}.amazonaws.com/${AWS::AccountId}/SVT-CustomerNotifications-ConvertToPDF-'+self.config['Env'])

        self.template.set_transform('AWS::Serverless-2016-10-31')
        self.template.add_resource(Function(
            function_logical_id,
            Description = 'Correspondence Customer Notifications ConvertToPDF Lambda',
            FunctionName = function_name,
            Environment = Environment(
                Variables = environment_variables
            ),
            PackageType = 'Image',
            ImageUri =  Sub('${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/' + str.lower(function_name) + ':latest'),
            Role = GetAtt(function_logical_id + 'Role', 'Arn'),
            AutoPublishAlias = self.config['Env'],
            MemorySize = self.config['LambdaMemorySize'],
            Timeout = self.config['LambdaTimeout'],
            Events = {
                'SQSEvent': {
                    'Type': 'SQS',
                    'Properties': {
                        'Enabled': True,
                        'Queue': GetAtt('Queue', 'Arn'),
                        'BatchSize': self.config['LambdaEventBatchSize'],
                        'MaximumBatchingWindowInSeconds': self.config['LambdaEventMaxBatchWindow']
                    }
                }
            },
            ReservedConcurrentExecutions = self.config['LambdaReservedConcurrentExecutions'],
            VpcConfig = VPCConfig(
				SecurityGroupIds = [self.config['LambdaSecurityGroup']],
				SubnetIds = [
					ImportValue("subnet-lambda-us-west-2a"),
					ImportValue("subnet-lambda-us-west-2b")
				]
			),
            Tracing = 'Active'
        ))
        
        self.template.add_resource(Function(
            Correspondencefunction_logical_id,
            Description = 'Correspondence ConvertToPDF Lambda',
            FunctionName = Correspondencefunction_name,
            Environment = Environment(
                Variables = Correspondence_env_variables
            ),
            PackageType = 'Image',
            ImageUri =  Sub('${AWS::AccountId}.dkr.ecr.${AWS::Region}.amazonaws.com/' + str.lower(Correspondencefunction_name) + ':latest'), # make sure docer image is present with this name
            Role = GetAtt(Correspondencefunction_logical_id + 'Role', 'Arn'),
            AutoPublishAlias = self.config['Env'],
            MemorySize = self.config['LambdaMemorySize'],
            Timeout = self.config['LambdaTimeout'],
            ReservedConcurrentExecutions = self.config['LambdaReservedConcurrentExecutions'],
            VpcConfig = VPCConfig(
				SecurityGroupIds = [self.config['LambdaSecurityGroup']],
				SubnetIds = [
					ImportValue("subnet-lambda-us-west-2a"),
					ImportValue("subnet-lambda-us-west-2b")
				]
			),
            Tracing = 'Active'
        ))

        self.template.add_resource(LogGroup(
            function_logical_id + 'LogGroup',
            LogGroupName = ''.join(['/aws/lambda/', function_name]),
            RetentionInDays = self.config['LambdaLogRetentionDays']
        ))
       
        # Correspondencefunction log group
        self.template.add_resource(LogGroup(
            Correspondencefunction_logical_id + 'LogGroup',
            LogGroupName = ''.join(['/aws/lambda/', Correspondencefunction_name]),
            RetentionInDays = self.config['LambdaLogRetentionDays']
        ))
        
        sfn_sub_dict={"functionName" : GetAtt(Correspondencefunction_logical_id , 'Arn')}
        
        self.template.add_resource(StateMachine(
        sfn_logical_id,
        Definition=self.Correspondence_sfn_defnition()
        DefinitionSubstitutions = sfn_sub_dict,
        RoleArn = GetAtt(CorrespondenceSfnName + 'Role', 'Arn'),
        StateMachineName = sfn_name
        )
        

    def create_template(self):
        self.initialize_template()
        self.CustomerNotificationsConvertToPDF()
        return self.template
