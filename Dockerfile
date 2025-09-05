FROM gcr.io/distroless/cc-debian12:nonroot

ENV FSS_PATH="/data"

# Copy the pre-built binary
COPY objectstore /bin/objectstore

VOLUME ["/data"]
ENTRYPOINT ["/bin/objectstore"]
EXPOSE 8888
