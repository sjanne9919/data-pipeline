import boto3
import time

name = ""
crawlerName = ""
crawlerRole = "" # Aws Glue service role to acess s3 and athena
modelDatabase = "dp_" + name + "_models"
reportDatabase = "dp_" + name + "_reports"

models = ["model2", "model1"]
reports = ["report1"]

SERVICE_NAME = "glue"
ACCESS_KEY = ""
SECRET_KEY = ""
REGION = ""  # us-west-1, ap-south-1

pathTemplate = "s3://com-data-pipeline" + name + "/query/#module#/#moduleName#/"


def get_client(serviceName, accessKey, secretKey, region):
    try:
        client = boto3.client(service_name=serviceName, aws_access_key_id=accessKey,
                              aws_secret_access_key=secretKey, region_name=region,
                              endpoint_url='https://' + serviceName + '.' + region + '.amazonaws.com')
    except Exception as err:
        print("ERROR 0003; Error while creating " + serviceName + " client. " + str(err))
    return client


def get_glue_client(accessKey=None, secretKey=None, region=None):
    if accessKey is None:
        accessKey = ACCESS_KEY
    if secretKey is None:
        secretKey = SECRET_KEY
    if region is None:
        region = REGION
    return get_client(SERVICE_NAME, accessKey, secretKey, region)


def update_and_run_crawler(crawlerName, crawlerRole, database, paths, forceRun, awaitCompletion=False, glueClient=None):
    if glueClient is None:
        glueClient = get_glue_client()
    exists = create_crawler(crawlerName, crawlerRole, database, paths, glueClient)
    updated = True
    if exists:
        updated = update_crawler(crawlerName, crawlerRole, database, paths, glueClient)
    else:
        print("New crawler " + crawlerName + " added.")

    if forceRun is "true" or updated:
        print("Running crawler " + crawlerName)
        run_crawler(crawlerName, awaitCompletion, glueClient)
    else:
        print("Skipping run for crawler " + crawlerName + " as no new paths added.")


def create_crawler(crawlerName, crawlerRole, database, paths, glueClient=None):
    try:
        if glueClient is None:
            glueClient = get_glue_client()
        glueClient.create_crawler(
            Name=crawlerName,
            Role=crawlerRole,
            DatabaseName=database,
            Description='crawl_documents_reports',
            Targets={
                'S3Targets': prepare_s3_target_paths(paths)
            }
        )
    except Exception as err:
        if 'already exists' in str(err):
            return True
        else:
            print("Error while creating  the crawler", str(err))
    return False


def delete_crawler(crawlerName, glueClient=None):
    try:
        if glueClient is None:
            glueClient = get_glue_client()
        glueClient.delete_crawler(
            Name=crawlerName,
        )
    except Exception as err:
        print("Error while deleting  the crawler", str(err))


def prepare_s3_target_paths(paths):
    s3TargetPaths = []
    for path in paths:
        s3TargetPaths.append({'Path': path})
    return s3TargetPaths


def update_crawler(crawlerName, crawlerRole, database, paths, glueClient=None):
    updated = False
    if glueClient is None:
        glueClient = get_glue_client()

    currentPaths = get_current_crawler_paths(crawlerName, glueClient)
    pathsToAdd = list(set(paths) - set(currentPaths))

    newPaths = currentPaths + pathsToAdd

    if len(pathsToAdd) > 0:
        glueClient.update_crawler(
            Name=crawlerName,
            Role=crawlerRole,
            DatabaseName=database,
            Description='crawler to create document tables and reports tables first time',
            Targets={
                'S3Targets': prepare_s3_target_paths(newPaths)
            }
        )
        updated = True
    return updated


def get_current_crawler_paths(crawlerName, glueClient=None):
    if glueClient is None:
        glueClient = get_glue_client()

    crawler = glueClient.get_crawler(Name=crawlerName)
    pathsObj = crawler['Crawler']['Targets']['S3Targets']

    currentPaths = []

    for pathObj in pathsObj:
        path = pathObj["Path"]
        currentPaths.append(path)
    return currentPaths


def is_crawler_ready_for_run(crawlerName, glueClient=None):
    if glueClient is None:
        glueClient = get_glue_client()
    response = glueClient.get_crawler(
        Name=crawlerName
    )

    state = response['Crawler']['State']
    if "READY" in state:
        return True
    return False


def await_crawler_run_completion(crawlerName, glueClient=None):
    while is_crawler_ready_for_run(crawlerName) is not True:
        time.sleep(10)
        print(crawlerName + " running...")
    print(crawlerName + " run complete.")
    print("deleting crawler" + crawlerName)
    delete_crawler(crawlerName, glueClient)
    print("crawler: " + crawlerName + " deleted")


def run_crawler(crawlerName, awaitCompletion=False, glueClient=None):
    if glueClient is None:
        glueClient = get_glue_client()
    glueClient.start_crawler(
        Name=crawlerName
    )

    if awaitCompletion:
        await_crawler_run_completion(crawlerName, glueClient)


def prepareReportPaths(reports):
    paths = []
    for report in reports:
        path = pathTemplate.replace("#moduleName#", report).replace("#module#", "reports")
        paths.append(path)
    return paths


def prepareModelPaths(models):
    paths = []
    for model in models:
        path = pathTemplate.replace("#moduleName#", model).replace("#module#", "models")
        paths.append(path)
    return paths


def main():
    reportPaths = prepareReportPaths(reports)
    modelPaths = prepareModelPaths(models)

    update_and_run_crawler(crawlerName, crawlerRole, modelDatabase, modelPaths, "true")
    delete_crawler(crawlerName)
    # update_and_run_crawler(crawlerName, crawlerRole, reportDatabase, reportPaths, "true")
    # delete_crawler(crawlerName)

main()




