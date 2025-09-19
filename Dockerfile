FROM gcr.io/distroless/cc-debian12:debug-nonroot

ARG BINARY=objectstore
COPY ${BINARY} /bin/entrypoint

# Ensure correct permissions on the data volume
COPY --from=gcr.io/distroless/cc-debian12:nonroot --chown=nonroot:nonroot /home/nonroot /data

VOLUME ["/data"]
ENV FSS_PATH="/data"

ENTRYPOINT ["/bin/entrypoint"]
EXPOSE 8888
