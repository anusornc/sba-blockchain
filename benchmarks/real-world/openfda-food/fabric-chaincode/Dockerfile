FROM golang:1.22-alpine AS builder

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o /openfda-chaincode .

FROM alpine:3.20

ARG CC_SERVER_PORT=9999
ENV CHAINCODE_SERVER_ADDRESS=0.0.0.0:${CC_SERVER_PORT}
COPY --from=builder /openfda-chaincode /usr/local/bin/openfda-chaincode
EXPOSE ${CC_SERVER_PORT}
CMD ["openfda-chaincode"]
