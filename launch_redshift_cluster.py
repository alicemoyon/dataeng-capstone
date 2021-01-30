import json
import time
import boto3
import configparser


def launch_redshift_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('credentials.cfg'))

    KEY = config.get('AWS', 'AWS_ACCESS_KEY_ID')
    SECRET = config.get('AWS', 'AWS_SECRET_ACCESS_KEY')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    iam = boto3.client('iam',
                       aws_access_key_id=KEY,
                       aws_secret_access_key=SECRET,
                       region_name='eu-west-1'
                       )

    redshift = boto3.client('redshift',
                            region_name="eu-west-1",
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )

    # CREATE IAM ROLE that enable Redshift to access our S3 bucket
    # Step 1 - Create the IAM Role
    try:
        print("Creating a new IAM Role")
        dwhRole = iam.create_role(
            Path='/',
            RoleName=DWH_IAM_ROLE_NAME,
            Description="Allows Redshift clusters to call AWS services on your behalf.",
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    # Step 2 - Attach the policy
    print("Attaching AmazonS3ReadOnlyAccess Policy")

    iam.attach_role_policy(RoleName=DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
                          )['ResponseMetadata']['HTTPStatusCode']

    # Step 3 - get the IAM role ARN
    roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
    print('Role ARN is ', roleArn)

    #  CREATE REDSHIFT CLUSTER
    print("Creating Redshift cluster")
    try:
        redshift.create_cluster(
            # WH
            ClusterType=DWH_CLUSTER_TYPE,
            NodeType=DWH_NODE_TYPE,
            NumberOfNodes=int(DWH_NUM_NODES),

            # Identifiers & Credentials
            DBName=DWH_DB,
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            MasterUsername=DWH_DB_USER,
            MasterUserPassword=DWH_DB_PASSWORD,

            # Roles (for s3 access)
            IamRoles=[roleArn]
        )
    except Exception as e:
        print(e)

    # Retrieve cluster status and wait until it becomes available
    status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
    print("Cluster status is:", status, "- Waiting until cluster becomes available")
    while status != 'available':
        time.sleep(60)
        status = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
        print("Cluster status is:", status, "- Waiting until cluster becomes available")
    print("Retrieving endpoint and role arn")
    # Once the redshift cluster is available, retrieve endpoint and role arn
    cluster_desc = redshift.describe_clusters(ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
    DWH_ENDPOINT = cluster_desc['Endpoint']['Address']
    DWH_ROLE_ARN = cluster_desc['IamRoles'][0]['IamRoleArn']

    # Open incoming TCP port to access cluster endpoint
    print("Opening incoming TCP port to access cluster endpoint")
    try:
        ec2 = boto3.resource('ec2',
                             region_name="eu-west-1",
                             aws_access_key_id=KEY,
                             aws_secret_access_key=SECRET
                             )
        vpc = ec2.Vpc(id=cluster_desc['VpcId'])
        default_sg = list(vpc.security_groups.all())[0]
        print(default_sg)
        default_sg.authorize_ingress(
            GroupName='default',
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(DWH_PORT),
            ToPort=int(DWH_PORT)
        )
    except Exception as e:
        print(e)

    return DWH_ENDPOINT, DWH_ROLE_ARN




