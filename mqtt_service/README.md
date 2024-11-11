** Instructions for setup:

- ssh into EC2 instance
- scp files from "grp_service" to EC2
- run "install" bash script
- run tmux new -s window
- run python3 server.py --kafka-servers YOUR_MSK_BROKER_URL_HERE --pub-topic YOUR_KAFKA_TOPIC_HERE --configs PATH_TO_YOUR_CONFIG.PROPERTIES_FILE
    - For config.properties, this is created during the MSK setup tutorial on AWS (here: https://docs.aws.amazon.com/msk/latest/developerguide/create-topic.html)
- Close the EC2 window or terminal
- If you want to stop the server, connect to the EC2 instance again, run "tmux a -t window" then Ctrl+C to interrupt the script
