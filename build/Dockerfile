# syntax = docker/dockerfile:1.4.1

ARG GOVERSION=1.22.0-bullseye
ARG GCFLAGS=""
ARG LDFLAGS=""

#
# build!
#

FROM golang:${GOVERSION} as builder
RUN go install github.com/go-delve/delve/cmd/dlv@v1.22.1
WORKDIR /work
COPY ./go.mod ./
COPY ./go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod/ go mod download && mkdir /dest
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/knitd           -o /dest/knitd         .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/knitd_backend   -o /dest/knitd_backend .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/dataagt         -o /dest/dataagt       .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/volume_expander -o /dest/vex           .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/empty           -o /dest/empty         .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/log_recorder    -o /dest/log_recorder  .
RUN --mount=type=cache,target=/go/pkg/mod/ go build ${GCFLAGS:+-gcflags} ${GCFLAGS} ${LDFLAGS:+-trimpath -ldflags} ${LDFLAGS} -C ./cmd/loops           -o /dest/loops         .

FROM gcr.io/distroless/base-debian11:latest as base
USER nobody
WORKDIR /knit

FROM gcr.io/distroless/base-debian11:debug as debugger
USER nobody
WORKDIR /knit/debugger/dlv
WORKDIR /knit
ENV XDG_CONFIG_HOME=/knit/debugger
COPY --from=builder --chown=nobody:nobody /go/bin/dlv /go/bin/dlv

#
# package!
#
FROM base as knitd
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
VOLUME /knit/configs
ENTRYPOINT ["/knit/knitd"]
COPY --from=builder --chown=nobody:nobody /dest/knitd /knit/knitd

FROM debugger as knitd-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/knitd", "--"]
COPY --from=builder --chown=nobody:nobody /dest/knitd /knit/knitd


FROM base as knitd-backend
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
VOLUME /knit/configs
ENTRYPOINT ["/knit/knitd_backend"]
CMD ["--config", "/knit/configs/knit.backend.yml"]
COPY --from=builder --chown=nobody:nobody /dest/knitd_backend /knit/knitd_backend

FROM debugger as knitd-backend-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--log-output", "debugger,gdbwire,lldbout,debuglineerr,rpc,dap,fncall,minidump", "--headless", "-l", ":19999", "--api-version", "2", "/knit/knitd_backend", "--"]
CMD ["--config", "/knit/configs/knit.backend.yml"]
COPY --from=builder --chown=nobody:nobody /dest/knitd_backend /knit/knitd_backend


FROM base as knit-vex
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
VOLUME /knit/configs
ENTRYPOINT ["/knit/vex"]
CMD ["--storage", "5Gi", "--delta", "5Gi"]
COPY --from=builder --chown=nobody:nobody /dest/vex /knit/vex
# ENV KUBECONFIG=...

FROM debugger as knit-vex-debug
VOLUME /knit/configs
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/vex", "--"]
CMD ["--storage", "5Gi", "--delta", "5Gi"]
COPY --from=builder --chown=nobody:nobody /dest/vex /knit/vex


FROM base as knit-dataagt
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
EXPOSE 8080
VOLUME /knit/mount
ENTRYPOINT ["/knit/dataagt"]
CMD ["--port", "8080", "--path", "/knit/mount"]
COPY --from=builder --chown=nobody:nobody /dest/dataagt /knit/dataagt

FROM debugger as knit-dataagt-debug
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/dataagt", "--"]
CMD ["--port", "8080", "--path", "/knit/mount"]
COPY --from=builder --chown=nobody:nobody /dest/dataagt /knit/dataagt


FROM base as knit-empty
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
ENTRYPOINT ["/knit/empty"]
COPY --from=builder --chown=nobody:nobody /dest/empty /knit/empty

FROM debugger as knit-empty-debug
# ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/empty", "--"]
ENTRYPOINT ["/knit/empty"]
COPY --from=builder --chown=nobody:nobody /dest/empty /knit/empty


FROM base as knit-nurse
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
ENTRYPOINT ["/knit/log_recorder"]
COPY --from=builder --chown=nobody:nobody /dest/log_recorder /knit/log_recorder

FROM debugger as knit-nurse-debug
# ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/log_recorder", "--"]
ENTRYPOINT ["/knit/log_recorder"]
COPY --from=builder --chown=nobody:nobody /dest/log_recorder /knit/log_recorder


FROM base as knit-loops
LABEL org.opencontainers.image.source="https://github.com/opst/knitfab"
ENTRYPOINT ["/knit/loops"]
COPY --from=builder --chown=nobody:nobody /dest/loops /knit/loops

FROM debugger as knit-loops-debug
ENTRYPOINT ["/go/bin/dlv", "exec", "--continue", "--accept-multiclient", "--log", "--headless", "-l", ":19999", "--api-version", "2", "/knit/loops", "--"]
COPY --from=builder --chown=nobody:nobody /dest/loops /knit/loops
