f"""
This is the wrapper for spark-submit
This allows to do a spark-submit in local, remote or using Livy
"""
from airflow import settings
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator
from dynaconf import settings

which_env = settings.ENV
email_to = settings.EMAIL

SPARK_SUBMIT_LOCAL = "spark-submit  --conf spark.executor.heartbeatInterval=30s " \
               "--conf spark.network.timeout=80s --conf spark.yarn.maxAppAttempts=1 " \
               "--conf spark.shuffle.service.enabled=true"
SPARK_SUBMIT_CLUSTER = "spark-submit  --master yarn --deploy-mode cluster --conf spark.executor.heartbeatInterval=30s " \
               "--conf spark.network.timeout=80s --conf spark.yarn.maxAppAttempts=1 " \
               "--conf spark.shuffle.service.enabled=true"

def bash_for_spark_submit(python_file, job_name, dag, conn_id = '', env_var_size = 'medium', trigger_rule='all_success'):
    spark_params = settings.get(env_var_size)
    num_executors = spark_params['num-executors']
    total_executor_cores = spark_params['total-executor-cores']
    driver_cores = spark_params['driver-cores']
    driver_memory = spark_params['driver-memory']
    executor_memory = spark_params['executor-memory']
    if (which_env == 'local'):
        SPARK_SUBMIT = SPARK_SUBMIT_LOCAL
    else:
        SPARK_SUBMIT = SPARK_SUBMIT_CLUSTER
    operator = BashOperator(
        task_id=job_name,
        bash_command=" {{ params.spark_submit }} "
                     + " --num-executors " + str(num_executors)
                     + " --total-executor-cores " + str(total_executor_cores)
                     + " --driver-cores " + str(driver_cores)
                     + " --executor-memory " + str(executor_memory)
                     + " --driver-memory " + str(driver_memory)
                     + " --name {{params.job_name}} {{params.python_file}} ",

        params={"spark_submit": SPARK_SUBMIT,
                "job_name":job_name,
                "python_file":python_file},
        email_on_failure=True,
        email=email_to,
        dag=dag,
        trigger_rule=trigger_rule
    )
    return operator


def bash_for_ssh_spark_submit(python_file, job_name, dag, conn_id = '', env_var_size = 'medium', trigger_rule='all_success'):
    spark_params = settings.get(env_var_size)
    num_executors = spark_params['num-executors']
    total_executor_cores = spark_params['total-executor-cores']
    driver_cores = spark_params['driver-cores']
    driver_memory = spark_params['driver-memory']
    executor_memory = spark_params['executor-memory']
    if (which_env == 'local'):
        SPARK_SUBMIT = SPARK_SUBMIT_LOCAL
    else:
        SPARK_SUBMIT = SPARK_SUBMIT_CLUSTER
    operator = SSHOperator(
        ssh_conn_id=conn_id,
        task_id=job_name,
        command= " {{ params.spark_submit }} "
                     + " --num-executors " + str(num_executors)
                     + " --total-executor-cores " + str(total_executor_cores)
                     + " --driver-cores " + str(driver_cores)
                     + " --executor-memory " + str(executor_memory)
                     + " --driver-memory " + str(driver_memory)
                     + " --name {{params.job_name}} {{params.python_file}} ",

        params={"spark_submit": SPARK_SUBMIT,
                "job_name":job_name,
                "python_file":python_file},
        email_on_failure=True,
        email=email_to,
        dag=dag,
        trigger_rule=trigger_rule
    )
    return operator

def spark_submit(python_file, job_name, dag, conn_id, env_var_size, trigger_rule='all_success'):

    if which_env == "local":
        return bash_for_spark_submit(python_file, job_name, dag, conn_id, env_var_size, trigger_rule)
    elif which_env == "remote":
        return bash_for_ssh_spark_submit(python_file, job_name, dag, conn_id, env_var_size, trigger_rule)
    else:
        raise Exception("Set a variable in settings.toml with one of these values 'local: for local spark-submit', "
                        "'remote: for doing SSH and spark-submit' ")

