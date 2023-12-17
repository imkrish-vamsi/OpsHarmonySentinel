from deploy.os_cron_celery_async.connectors.os_query import get_llm_response
from dotenv import load_dotenv

load_dotenv()

if __name__ == "__main__":
    df = get_llm_response()
    df.to_csv('OHS-7b-alpha-zabbix-responses.csv', header=True, index=False)