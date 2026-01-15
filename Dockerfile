FROM gcr.io/distroless/cc-debian12:nonroot

ARG BINARY=objectstore
COPY ${BINARY} /bin/entrypoint
COPY sleep /bin/sleep

# Ensure correct permissions on the data volume
COPY --from=gcr.io/distroless/cc-debian12:nonroot --chown=nonroot:nonroot /home/nonroot /data

VOLUME ["/data"]

ENTRYPOINT ["/bin/entrypoint"]
CMD ["--help"]
EXPOSE 8888
