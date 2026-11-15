# Build from the repository root (the scheduler module imports the driver module via ../):
#   docker build -f scheduler/Dockerfile .
FROM --platform=$BUILDPLATFORM golang:1.26 AS build
ARG TARGETOS TARGETARCH
# Reported by /version and logs; keep in sync with k8s.io/kubernetes in scheduler/go.mod.
ARG KUBE_VERSION=v1.37.2
WORKDIR /src
COPY go.mod go.sum ./
COPY scheduler/go.mod scheduler/go.sum scheduler/
RUN cd scheduler && go mod download
COPY . .
RUN cd scheduler && CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags "-s -w -X k8s.io/component-base/version.gitVersion=${KUBE_VERSION} -X k8s.io/component-base/version.gitMajor=1 -X k8s.io/component-base/version.gitMinor=37" -o /out/hermit-scheduler ./cmd/hermit-scheduler

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/hermit-scheduler /usr/local/bin/hermit-scheduler
ENTRYPOINT ["/usr/local/bin/hermit-scheduler"]
