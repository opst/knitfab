ARG GOVERSION=1.24.1-bullseye

FROM golang:${GOVERSION} AS builder
WORKDIR /work
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod/ --mount=type=cache,target=/root/.cache/go-build \
    mkdir ./dest && \
    go build -o ./dest/task .

FROM gcr.io/distroless/base-debian11:latest
WORKDIR /task
COPY --from=builder --chown=nobody:nobody /work/dest/task /task/task
ENTRYPOINT ["/task/task"]
CMD ["-sleep", "600"]
