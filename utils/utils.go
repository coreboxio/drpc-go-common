package utils

import (
	"fmt"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/segmentio/ksuid"
)

func GetDeviceIP(ifaName string) string {
	interfaces, err := net.Interfaces()
	if err != nil {
		return ""
	}

	ipCache := ""
	for _, iface := range interfaces {
		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip != nil && !ip.IsLoopback() && ip.To4() != nil {
				if ifaName != "" && iface.Name == ifaName {
					return ip.String()
				}
				if ipCache == "" && ip.String() != "" {
					ipCache = ip.String()
				}

			}
		}
	}

	return ipCache
}

func GetIPv4(r *http.Request) string {
	xForwardedFor := r.Header.Get("X-Forwarded-For")
	if xForwardedFor != "" {
		parts := strings.Split(xForwardedFor, ",")
		for i, p := range parts {
			parts[i] = strings.TrimSpace(p)
		}
		return parts[0]
	}

	xRealIP := r.Header.Get("X-Real-IP")
	if xRealIP != "" {
		return xRealIP
	}

	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return ""
	}

	return ip
}

func GenUUID() string {
	return ksuid.New().String()
}

func GenNumberCode(length int) string {
	max := int32(1)
	for i := 0; i < length; i++ {
		max *= 10
	}
	min := max / 10
	return fmt.Sprintf("%0*d", length, rand.New(rand.NewSource(time.Now().UnixNano())).Int31n(max-min)+min)
}
