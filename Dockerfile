FROM node:22-alpine AS frontend-builder

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /src/web

COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY web/tsconfig.json web/next.config.ts web/components.json web/postcss.config.mjs web/eslint.config.mjs ./
COPY web/public ./public
COPY web/src ./src

RUN pnpm run build 2>&1; RC=$?; echo "=== NEXT BUILD EXIT CODE: $RC ==="; exit $RC

FROM golang:1.24-alpine AS go-builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY static/ ./static/
COPY --from=frontend-builder /src/web/out ./static/out

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    go build -ldflags="-s -w" -o /octopus main.go

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata su-exec wget && \
    ln -fs /usr/share/zoneinfo/Asia/Shanghai /etc/localtime && \
    echo "Asia/Shanghai" > /etc/timezone

WORKDIR /app
RUN mkdir -p /app/data

COPY --from=go-builder /octopus /app/octopus
COPY scripts/dockerfiles/entrypoint.sh /entrypoint.sh
RUN chmod +x /app/octopus /entrypoint.sh

ENV TZ=Asia/Shanghai
EXPOSE 8080
VOLUME ["/app/data"]
ENTRYPOINT ["/entrypoint.sh"]
