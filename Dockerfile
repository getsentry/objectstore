FROM gcr.io/distroless/cc-debian12:nonroot

ARG BINARY=objectstore
COPY ${BINARY} /bin/entrypoint

VOLUME ["/data"]
ENV FSS_PATH="/data"

ENTRYPOINT ["/bin/entrypoint"]
EXPOSE 8888
