FROM clojure:openjdk-8-lein

RUN mkdir -p /home/tap-mssql

WORKDIR /home/tap-mssql

COPY ./src /home/tap-mssql/src
COPY ./project.clj /home/tap-mssql/

RUN echo "#!/usr/bin/env bash" > tap-mssql && \
    echo "cd /home/tap-mssql" >> tap-mssql && \
    echo "lein run -m tap-mssql.core \"$@\"" >> tap-mssql && \
    chmod 777 tap-mssql

ENTRYPOINT [ "/home/tap-mssql/tap-mssql" ]
