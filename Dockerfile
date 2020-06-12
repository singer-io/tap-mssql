# Updates pushed via:
# > docker build -t dataopstk/tapdance:tap-mssql-raw .
# > docker push dataopstk/tapdance:tap-mssql-raw

FROM clojure:openjdk-8-lein

RUN mkdir -p /home/tap-mssql

WORKDIR /home/tap-mssql

COPY ./bin /home/tap-mssql/bin
COPY ./resources /home/tap-mssql/resources
COPY ./src /home/tap-mssql/src
COPY ./project.clj /home/tap-mssql/

# Installs files on first run:
RUN cd /home/tap-mssql && \
    ./bin/tap-mssql

ENV PATH "/home/tap-mssql/bin:${PATH}"

ENTRYPOINT [ "tap-mssql" ]
