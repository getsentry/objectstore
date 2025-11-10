FROM gcr.io/distroless/cc-debian12:nonroot

ARG BINARY=objectstore
COPY ${BINARY} /bin/entrypoint

# Ensure correct permissions on the data volume
COPY --from=gcr.io/distroless/cc-debian12:nonroot --chown=nonroot:nonroot /home/nonroot /data

VOLUME ["/data"]
ENV OS__HIGH_VOLUME_STORAGE__PATH="/data"

ENTRYPOINT ["/bin/entrypoint"]
CMD ["--help"]
EXPOSE 8888
