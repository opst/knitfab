# syntax = docker/dockerfile:1.4.1

ARG GOVERSION=1.22.0-bullseye
ARG GCFLAGS=""
ARG LDFLAGS=""
ARG REPOSITORY_PATH=""

#
# build!
#

FROM --platform=${TARGETPLATFORM} golang:${GOVERSION} as builder
WORKDIR /work
COPY ./go.mod ./
COPY ./go.sum ./
ENV GOCACHE=/root/.cache/go-build GOOS=linux
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod/ --mount=type=cache,target=/root/.cache/go-build \
    mkdir /dest && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/knitd           -o /dest/knitd         . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/knitd_backend   -o /dest/knitd_backend . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/dataagt         -o /dest/dataagt       . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/volume_expander -o /dest/vex           . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/empty           -o /dest/empty         . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/log_recorder    -o /dest/log_recorder  . && \
    go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/loops           -o /dest/loops         .

FROM --platform=${TARGETPLATFORM} golang:${GOVERSION} as dlv-installer
RUN go install github.com/go-delve/delve/cmd/dlv@v1.22.1

FROM --platform=${TARGETPLATFORM} gcr.io/distroless/base-debian11:latest as base
USER nobody
WORKDIR /knit

FROM --platform=${TARGETPLATFORM} gcr.io/distroless/base-debian11:debug as debugger
USER nobody
WORKDIR /knit/debugger/dlv
WORKDIR /knit
ENV XDG_CONFIG_HOME=/knit/debugger
COPY --from=dlv-installer --chown=nobody:nobody /go/bin/dlv /go/bin/dlv

#
# package!
#
FROM --platform=${TARGETPLATFORM} base as knitd
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
VOLUME /knit/configs
ENTRYPOINT ["/knit/knitd"]
COPY --from=builder --chown=nobody:nobody /dest/knitd /knit/knitd

FROM --platform=${TARGETPLATFORM} debugger as knitd-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/knitd", "--"]
COPY --from=builder --chown=nobody:nobody /dest/knitd /knit/knitd


FROM --platform=${TARGETPLATFORM} base as knitd-backend
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
VOLUME /knit/configs
ENTRYPOINT ["/knit/knitd_backend"]
CMD ["--config", "/knit/configs/knit.backend.yml"]
COPY --from=builder --chown=nobody:nobody /dest/knitd_backend /knit/knitd_backend

FROM --platform=${TARGETPLATFORM} debugger as knitd-backend-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--log-output", "debugger,gdbwire,lldbout,debuglineerr,rpc,dap,fncall,minidump", "--headless", "-l", ":19999", "--api-version", "2", "/knit/knitd_backend", "--"]
CMD ["--config", "/knit/configs/knit.backend.yml"]
COPY --from=builder --chown=nobody:nobody /dest/knitd_backend /knit/knitd_backend


FROM --platform=${TARGETPLATFORM} base as knit-vex
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
VOLUME /knit/configs
ENTRYPOINT ["/knit/vex"]
CMD ["--storage", "5Gi", "--delta", "5Gi"]
COPY --from=builder --chown=nobody:nobody /dest/vex /knit/vex
# ENV KUBECONFIG=...

FROM --platform=${TARGETPLATFORM} debugger as knit-vex-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/vex", "--"]
CMD ["--storage", "5Gi", "--delta", "5Gi"]
COPY --from=builder --chown=nobody:nobody /dest/vex /knit/vex


FROM --platform=${TARGETPLATFORM} base as knit-dataagt
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
EXPOSE 8080
VOLUME /knit/mount
ENTRYPOINT ["/knit/dataagt"]
CMD ["--port", "8080", "--path", "/knit/mount"]
COPY --from=builder --chown=nobody:nobody /dest/dataagt /knit/dataagt

FROM --platform=${TARGETPLATFORM} debugger as knit-dataagt-debug
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/dataagt", "--"]
CMD ["--port", "8080", "--path", "/knit/mount"]
COPY --from=builder --chown=nobody:nobody /dest/dataagt /knit/dataagt


FROM --platform=${TARGETPLATFORM} base as knit-empty
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
ENTRYPOINT ["/knit/empty"]
COPY --from=builder --chown=nobody:nobody /dest/empty /knit/empty

FROM --platform=${TARGETPLATFORM} debugger as knit-empty-debug
# ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/empty", "--"]
ENTRYPOINT ["/knit/empty"]
COPY --from=builder --chown=nobody:nobody /dest/empty /knit/empty


FROM --platform=${TARGETPLATFORM} base as knit-nurse
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
ENTRYPOINT ["/knit/log_recorder"]
COPY --from=builder --chown=nobody:nobody /dest/log_recorder /knit/log_recorder

FROM --platform=${TARGETPLATFORM} debugger as knit-nurse-debug
# ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/log_recorder", "--"]
ENTRYPOINT ["/knit/log_recorder"]
COPY --from=builder --chown=nobody:nobody /dest/log_recorder /knit/log_recorder


FROM --platform=${TARGETPLATFORM} base as knit-loops
ARG REPOSITORY_PATH
LABEL org.opencontainers.image.source="https://github.com/${REPOSITORY_PATH}"
ENTRYPOINT ["/knit/loops"]
COPY --from=builder --chown=nobody:nobody /dest/loops /knit/loops

FROM --platform=${TARGETPLATFORM} debugger as knit-loops-debug
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/loops", "--"]
COPY --from=builder --chown=nobody:nobody /dest/loops /knit/loops
