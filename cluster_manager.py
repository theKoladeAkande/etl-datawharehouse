import time
import json
import boto3
import config


def create_ec2_resource():
    """
    Connects to aws user to get resources for
    ec2.
    """
    global ec2
    ec2 = boto3.resource('ec2',
                         region_name='us-west-2',
                         aws_access_key_id=config.KEY,
                         aws_secret_access_key=config.SECRET,
                        )



def create_s3_resource():
    """
    Connects to  aws user to get resources for
    s3.
    """
    global s3
    s3 = boto3.resource('s3',
                        region_name = 'us-west-2',
                        aws_access_key_id=config.KEY,
                        aws_secret_access_key=config.SECRET)
    return s3


def create_iam_access():
    """
    Connects to aws user to get resources for
    iam privileges, creates iam role to allow redshift
    cluster access aws services, enables redshift read
    only access to s3
    """
    global iam
    global roleArn
    iam = boto3.client('iam',
                     region_name = 'us-west-2',
                     aws_access_key_id=config.KEY,
                     aws_secret_access_key=config.SECRET,
                     )

    try:
        print('Creating Iam role...')
        dwhRole = iam.create_role(
                Path='/',
                RoleName=config.DWH_IAM_ROLE_NAME,
                Description="Allow redshift clusters to access AWS services",
                AssumeRolePolicyDocument=json.dumps(
                    {'Statement': [
                        {'Action': 'sts:AssumeRole',
                        'Effect': 'Allow',
                        'Principal': {'Service': 'redshift.amazonaws.com'}}],
                        'Version': '2012-10-17'}))

    except Exception as e:
         print(e)

    print('attaching policy...')


    iam.attach_role_policy(RoleName=config.DWH_IAM_ROLE_NAME,
                       PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")\
                           ['ResponseMetadata']['HTTPStatusCode']


    print(" get the I am role ARN")
    roleArn = iam.get_role(RoleName=config.DWH_IAM_ROLE_NAME)['Role']['Arn']



def launch_redshift_cluster():
    """
    Connects to  aws user to get resources for redshift,
    creates redshift cluster with iam privleges, connects
    to clusters to get information for configurations,
    opens up TCP port for access to redshift from third-party
    softwares
    """
    global redshift
    redshift = boto3.client('redshift',
                            region_name="us-west-2",
                            aws_access_key_id=config.KEY,
                            aws_secret_access_key=config.SECRET
                       )
    try:
        response = redshift.create_cluster(
            #HW
            ClusterType=config.DWH_CLUSTER_TYPE,
            NodeType=config.DWH_NODE_TYPE,
            NumberOfNodes=int(config.DWH_NUM_NODES),

            #Identifiers & Credentials
            DBName=config.DWH_DB,
            ClusterIdentifier=config.DWH_CLUSTER_IDENTIFIER,
            MasterUsername=config.DWH_DB_USER,
            MasterUserPassword=config.DWH_DB_PASSWORD,

            #Roles (for s3 access)
            IamRoles=[roleArn])

    except Exception as e:
        print(e)

    #try connecting to cluster, if available if not wait and try again
    not_available = True
    while not_available:
        try:
            print('waiting for clusters to be available....give us 60seconds')
            time.sleep(60)
            print('Connecting...!')
            my_cluster_props = \
                redshift.describe_clusters(ClusterIdentifier=config.DWH_CLUSTER_IDENTIFIER)['Clusters'][0]

            config.DWH_ENDPOINT = my_cluster_props['Endpoint']['Address']
            config.DWH_ROLE_ARN = my_cluster_props['IamRoles'][0]['IamRoleArn']
            not_available=False
            print('Connected')

        except Exception as e:
            print('Oops!!cluster not yet available')

    try:
        vpc = ec2.Vpc(id=my_cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[0]
        print(defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(config.DWH_PORT),
            ToPort=int(config.DWH_PORT)
        )
    except Exception as e:
         print(e)

    print('Redshift cluster up and running...')


def teardown_redshift_cluster():
    """ Tears down redshift clusters"""
    try:
        redshift.delete_cluster(ClusterIdentifier=config.DWH_CLUSTER_IDENTIFIER,
                                SkipFinalClusterSnapshot=True)
        print('Redshift cluster deleted')
    except Exception as e:
        print(e)


def remove_iam_access():
    """ Delete roles and detach policies"""
    iam.detach_role_policy(RoleName=config.DWH_IAM_ROLE_NAME,
                           PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess")
    iam.delete_role(RoleName=config.DWH_IAM_ROLE_NAME)
    print("Iam access removed and detached")