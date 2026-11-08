# Redis Stream consumer that retrieves and processes queued jobs
# as part of a consumer group.

from shared.redis_client import redis_client
from redis.exceptions import ResponseError
import redis
import time
import csv

# Create a worker group
try:
    redis_client.xgroup_create(
        name="jobs", # look through this stream
        groupname="csv-workers", # name our group of workers
        id=0, # first index to start from in the queue
        mkstream=True # create stream if it doesnt exist
    )

except ResponseError as e:
    if "BUSYGROUP" not in str(e): # Group exists already
        raise


# always listen for new jobs
while True:
    try:
        response = redis_client.xreadgroup(
            groupname="csv-workers",
            consumername="worker-1",
            streams={"jobs": ">"},
            count=1, # number of messages it can retrieve per read
            block=2000
        )

        if not response:
            continue # no messages avaialable

        # Unpack the response
        for stream, messages in response:
            for message_id, message_data in messages:
                print(f"Got Message: {message_data}")

                job_id = message_data["job_id"]
                filename = message_data["filename"]

                # Processing logic...
                print(f"Processing {job_id}: {filename}")
                time.sleep(1)

                # send the acknoledgement
                redis_client.xack("jobs", "csv-workers", message_id)
                print(f"Acknowledged {job_id} processing {filename}")

    except redis.exceptions.ConnectionError:
        print("Error: Could not connect to redis.")
        time.sleep(1)

    except ResponseError as e:
        print(f"Error: {e}")
        time.sleep(1)
