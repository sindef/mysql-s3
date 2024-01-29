from golang:1.22rc2-alpine3.19 as builder

WORKDIR /go/src/mysql-s3
COPY . .
RUN go get ./...
RUN go build -o /go/bin/mysql-s3

FROM ubuntu:22.04

RUN apt update && apt install -y mariadb-client

COPY --from=builder /go/bin/mysql-s3 /usr/local/bin/mysql-s3

ENTRYPOINT ["/usr/local/bin/mysql-s3"]
