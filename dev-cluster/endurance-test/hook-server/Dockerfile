ARG GOVERSION=1.24.1-bullseye
FROM golang:${GOVERSION} AS builder
WORKDIR /work
ENV HOME=/work
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod/ --mount=type=cache,target=/root/.cache/go-build \
    mkdir ./dest && \
    go mod tidy && \
    go build -o ./dest/hook . && \
    ./knit init ./knitprofile


FROM gcr.io/distroless/base-debian11:latest
WORKDIR /hook
COPY --from=builder --chown=nobody:nobody /work/dest/hook /hook/hook
COPY --from=builder --chown=nobody:nobody /work/knit /hook/knit
COPY --from=builder --chown=nobody:nobody /work/.knitprofile /hook/.knitprofile
COPY --from=builder --chown=nobody:nobody /work/.knit /hook/.knit
ENV PATH=/hook:$PATH
ENV HOME=/hook
ENTRYPOINT ["/hook/hook"]
