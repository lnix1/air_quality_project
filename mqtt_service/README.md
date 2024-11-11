** Instructions for setup:

- ssh into EC2 instance
- scp files from "grp_service" to EC2
- run "install" bash script
- run python3 server.py --kafka-servers YOUR_MSK_BROKER_URL_HERE --pub-topic YOUR_KAFKA_TOPIC_HERE --configs PATH_TO_YOUR_CONFIG.PROPERTIES_FILE
    - For config.properties, this is created during the MSK setup tutorial on AWS (here: https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html)
