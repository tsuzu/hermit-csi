FROM golang:1.26 AS build
ARG VERSION=dev
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/hermit-csi ./cmd/hermit-csi

FROM ubuntu:24.04
RUN apt-get update \
 && apt-get install -y --no-install-recommends lvm2 thin-provisioning-tools e2fsprogs xfsprogs util-linux ca-certificates \
 && rm -rf /var/lib/apt/lists/*
# LVM runs inside a privileged container without udev: let it manage /dev nodes itself.
RUN sed -E -i 's/^(\s*)(#\s*)?(udev_sync|udev_rules|obtain_device_list_from_udev) = [01]$/\1\3 = 0/' /etc/lvm/lvm.conf \
 && test "$(lvmconfig --typeconfig full activation/udev_sync)" = "udev_sync=0" \
 && test "$(lvmconfig --typeconfig full activation/udev_rules)" = "udev_rules=0"
COPY --from=build /out/hermit-csi /usr/local/bin/hermit-csi
ENTRYPOINT ["/usr/local/bin/hermit-csi"]
