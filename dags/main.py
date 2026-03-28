from airflow import DAG
import pendulum
from datetime import datetime
from datetime import timedelta

from api.video_stats import get_playlistid, get_video_ids, extract_video_data, save_to_json

#define local timezone
local_tz=pendulum.timezone("Africa/Cairo")


# Default Args
default_args = {
    "owner": "hashimasaad",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "email": "data@engineers.com",
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=5),
    "max_active_runs": 1,
    "dagrun_timeout": timedelta(hours=1),
    "start_date": datetime(2026, 1, 1, tzinfo=local_tz),
    # 'end_date': datetime(2030, 12, 31, tzinfo=local_tz),
}

# DAG 1: produce_json
with DAG(
    dag_id="produce_json",
    default_args=default_args,
    description="DAG to produce JSON file with raw data",
    schedule="0 14 * * *",
    catchup=False,
) as dag_produce:
    # Define tasks
    playlist_id = get_playlistid()
    video_ids = get_video_ids(playlist_id)
    extract_data = extract_video_data(video_ids)
    save_to_json_task = save_to_json(extract_data)

     
    # Define dependencies
    playlist_id >> video_ids >> extract_data >> save_to_json_task 