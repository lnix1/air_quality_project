sudo yum -y install python-pip

python3 -m pip install grpcio grpcio-tools paho-mqtt

python3 -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. dustsensor.proto

python3 -m pip install kafka-python

python3 -m pip install typing_extensions
