FROM public.ecr.aws/lambda/python:3.11

# Copy requirements.txt
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Install git
RUN yum install -y openssl-devel-1.0.2k-24.amzn2.0.15
RUN yum install -y git-all

# Install the specified packages
RUN pip install -r requirements.txt

# Copy function code
COPY service.py ${LAMBDA_TASK_ROOT}

# Set the CMD to your handler (could also be done as a parameter override outside of the Dockerfile)
CMD [ "service.handler" ]
