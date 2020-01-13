# FPG AIRFLOW
# Author:DC (dc@accionlabs.com)
## Description

Airflow Dag for Data Lake and potentially Fact Processing and PMS extract

## How do Deploy
 
1. Install Airflow
* $ python3 --version
* Python 3.6.8
* $ virtualenv --version
* 16.7.5
* cd
* mkdir airflow
* cd ~/airflow
* virtualenv -p `which python3` venv
* source venv/bin/activate
* pip install apache-airflow
* pip install psycopg2	2.7.5
* Clone this repo under $AIRFLOW_HOME
* Open another terminal - cd ~/airflow; export AIRFLOW_HOME=~/airflow; airflow initdb; airflow webserver
* Open another terminal - cd ~/airflow; export AIRFLOW_HOME=~/airflow; airflow scheduler
* The dag fpg_main_dag.py should appear in localhost:8080
* Do following changes in AIRFLOW_HOME/airflow.cfg
  ** dags_folder = <your_dir>/fpg-airflow
  ** sql_alchemy_conn = postgresql+psycopg2://__dc@localhost:5432/dc__  to your postgres instance
  ** executor = LocalExecutor
  ** smtp_host = smtp.sendgrid.net
  **smtp_starttls = False
  **smtp_ssl = True
  **smtp_user = apikey
  **smtp_password = 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
  **smtp_port = 465
  **smtp_mail_from = airflow@accionlabs.com
* Add following in ~/bash_profile export SETTINGS_FILE_FOR_DYNACONF='["<your_dir>/settings.toml"]'


### How to run

* Manually trigger fpg_main_dag provided.
* If everything is set up it will send a email to the email provided in settings.toml

### What is demonstrated in this
* Running Python jobs
* Running Bash Jobs
* Running a spark submit (managed using common functions)
* Using xcom
* Orchestration between jobs
* Sending emails when job fails
 