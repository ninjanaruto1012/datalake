FROM python:3.8.0

RUN pip install mlflow pandas==1.5.3 flask cloudpickle==1.6.0 psutil==5.8.0 scikit-learn==0.24.1 boto3 tensorflow