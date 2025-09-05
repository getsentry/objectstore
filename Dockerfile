FROM gcr.io/distroless/cc-debian12:nonroot

ARG BINARY=objectstore
COPY ${BINARY} /bin/application

VOLUME ["/data"]
ENV FSS_PATH="/data"

ENTRYPOINT ["/bin/application"]
EXPOSE 8888
