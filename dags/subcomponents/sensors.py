import logging
from typing import Dict

from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

DAILY_FEEDS: Dict[str, str] = {
    "pos":       "pos_sales_{{ ds_nodash }}.csv",
    "inventory": "inventory_snapshot_{{ ds_nodash }}.csv",
    "item":      "item_master_{{ ds_nodash }}.csv",
    "store":     "store_master_{{ ds_nodash }}.csv",
}

def build_wait_sensors(group_id: str, base_dir: str) -> TaskGroup:

    with TaskGroup(group_id=group_id) as sensor_group:

        for feed_key, filename_template in DAILY_FEEDS.items():

            full_path = f"{base_dir.rstrip('/')}/{filename_template}"

            logging.debug(
                "Registering FileSensor: feed=%s  path_template=%s",
                feed_key, full_path
            )

            FileSensor(

                task_id=f"wait_for_{feed_key}",

                filepath=full_path,

                fs_conn_id="fs_default",

                poke_interval=1,

                timeout=1,

                mode="reschedule",

                soft_fail=False,
            )

    return sensor_group
