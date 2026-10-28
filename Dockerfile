ARG BASE_IMAGE=us-docker.pkg.dev/sentryio/dhi-mirror/static:20250419-glibc-debian13
FROM ${BASE_IMAGE} AS base

FROM ${BASE_IMAGE}

ARG BINARY=objectstore
COPY ${BINARY} /bin/entrypoint

# The base image has no shell to `mkdir` the data volume, so copy the empty
# home directory instead to ensure correct permissions.
COPY --from=base --chown=nonroot:nonroot /home/nonroot /data

VOLUME ["/data"]

ENTRYPOINT ["/bin/entrypoint"]
CMD ["--help"]
EXPOSE 8888
