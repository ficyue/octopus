# ============================================================
# Stage 1: Build frontend (Next.js static export)
# ============================================================
FROM node:22-alpine AS frontend-builder

RUN corepack enable && corepack prepare pnpm@latest --activate

WORKDIR /src/web

COPY web/package.json web/pnpm-lock.yaml web/pnpm-workspace.yaml ./
RUN pnpm install --frozen-lockfile

COPY web/tsconfig.json web/next.config.ts web/components.json web/postcss.config.mjs web/eslint.config.mjs ./
COPY web/public ./public
COPY web/src ./src

ENV NEXT_TELEMETRY_DISABLED=1
RUN pnpm run build && ls -la /src/web/out/

# ============================================================
# Stage 2: Build Go binary with embedded static files
# ============================================================
FROM golang:1.24-alpine AS go-builder

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY main.go ./
COPY cmd/ ./cmd/
COPY internal/ ./internal/
COPY static/ ./static/
COPY --from=frontend-builder /src/web/out ./static/out

RUN ls -la ./static/out/ | head -5

ARG VERSION=dev
ARG COMMIT=unknown
ARG BUILD_TIME=unknown
ENV CGO_ENABLED=0

RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    go build \
      -ldflags="-X 'github.com/bestruirui/octopus/internal/conf.Version=${VERSION}' -X 'github.com/bestruirui/octopus/internal/conf.BuildTime=${BUILD_TIME}' -X 'github.com/bestruirui/octopus/internal/conf.Commit=${COMMIT}' -s -w" \
      -o /octopus \
      main.go

# ============================================================
# Stage 3: Runtime
# ============================================================
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
ENV OCTOPUS_SERVER_HOST=0.0.0.0
ENV OCTOPUS_SERVER_PORT=8080

EXPOSE 8080
VOLUME ["/app/data"]

ENTRYPOINT ["/entrypoint.sh"]
