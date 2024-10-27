FROM python:3.11.2

# Install necessary packages
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        curl \
        unzip && \
    rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" && \
    unzip awscliv2.zip && \
    ./aws/install && \
    rm -rf awscliv2.zip aws

# Verify AWS CLI installation
RUN aws --version

RUN mkdir /app

WORKDIR /app

COPY . .

RUN pip install -r requirements.txt

WORKDIR /app/scripts

CMD [ "bash" ]

# ENTRYPOINT [ "python", "-u", "test.py" ]