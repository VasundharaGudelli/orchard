FROM alpine

RUN apk --no-cache --update add ca-certificates
RUN apk --no-cache add curl

COPY go/ .
RUN chmod +x bin/orchard
ENTRYPOINT ["bin/orchard"]

