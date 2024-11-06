sudo yum -y install python-pip

pip install grpcio grpcio-tools paho-mqtt

python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. your_proto_file.proto

pip install kafka-python
