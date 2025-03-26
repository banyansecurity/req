package req

import (
	"crypto/rand"
	"encoding/binary"
	"maps"
	"math/big"
	"net/http"
	"strconv"
	"strings"

	"github.com/imroc/req/v3/http2"
	utls "github.com/refraction-networking/utls"
)

// Identical for both Blink-based browsers (Chrome, Chromium, etc.) and WebKit-based browsers (Safari, etc.)
// Blink implementation: https://source.chromium.org/chromium/chromium/src/+/main:third_party/blink/renderer/platform/network/form_data_encoder.cc;drc=1d694679493c7b2f7b9df00e967b4f8699321093;l=130
// WebKit implementation: https://github.com/WebKit/WebKit/blob/47eea119fe9462721e5cc75527a4280c6d5f5214/Source/WebCore/platform/network/FormDataBuilder.cpp#L120
func webkitMultipartBoundaryFunc() string {
	const letters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789AB"

	sb := strings.Builder{}
	sb.WriteString("----WebKitFormBoundary")

	for i := 0; i < 16; i++ {
		index, err := rand.Int(rand.Reader, big.NewInt(int64(len(letters)-1)))
		if err != nil {
			panic(err)
		}

		sb.WriteByte(letters[index.Int64()])
	}

	return sb.String()
}

// Firefox implementation: https://searchfox.org/mozilla-central/source/dom/html/HTMLFormSubmission.cpp#355
func firefoxMultipartBoundaryFunc() string {
	sb := strings.Builder{}
	sb.WriteString("-------------------------")

	for i := 0; i < 3; i++ {
		var b [8]byte
		if _, err := rand.Read(b[:]); err != nil {
			panic(err)
		}
		u32 := binary.LittleEndian.Uint32(b[:])
		s := strconv.FormatUint(uint64(u32), 10)

		sb.WriteString(s)
	}

	return sb.String()
}

var (
	chromeHttp2Settings = []http2.Setting{
		{
			ID:  http2.SettingHeaderTableSize,
			Val: 65536,
		},
		{
			ID:  http2.SettingEnablePush,
			Val: 0,
		},
		{
			ID:  http2.SettingMaxConcurrentStreams,
			Val: 1000,
		},
		{
			ID:  http2.SettingInitialWindowSize,
			Val: 6291456,
		},
		{
			ID:  http2.SettingMaxHeaderListSize,
			Val: 262144,
		},
	}

	chromeCustomHttp2Settings = []http2.Setting{
		{
			ID:  http2.SettingHeaderTableSize,
			Val: 65536,
		},
		{
			ID:  http2.SettingEnablePush,
			Val: 0,
		},
		{
			ID:  http2.SettingMaxConcurrentStreams,
			Val: 1000,
		},
		// TODO: This seems to break custom client hellos for Chrome. As far as I
		// can tell, this seems to cause a HTTP 206 flow to begin which breaks
		// proxying.
		//
		// {
		// 	ID:  http2.SettingInitialWindowSize,
		// 	Val: 6291456,
		// },
		{
			ID:  http2.SettingMaxHeaderListSize,
			Val: 262144,
		},
	}

	chromePseudoHeaderOrder = []string{
		":method",
		":authority",
		":scheme",
		":path",
	}

	chromeHeaderOrder = []string{
		"host",
		"pragma",
		"cache-control",
		"sec-ch-ua",
		"sec-ch-ua-mobile",
		"sec-ch-ua-platform",
		"upgrade-insecure-requests",
		"user-agent",
		"accept",
		"sec-fetch-site",
		"sec-fetch-mode",
		"sec-fetch-user",
		"sec-fetch-dest",
		"referer",
		"accept-encoding",
		"accept-language",
		"cookie",
	}

	chromeHeaders = map[string]string{
		"pragma":                    "no-cache",
		"cache-control":             "no-cache",
		"sec-ch-ua":                 `"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"`,
		"sec-ch-ua-mobile":          "?0",
		"sec-ch-ua-platform":        `"macOS"`,
		"upgrade-insecure-requests": "1",
		"user-agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		"accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
		"sec-fetch-site":            "none",
		"sec-fetch-mode":            "navigate",
		"sec-fetch-user":            "?1",
		"sec-fetch-dest":            "document",
		"accept-language":           "zh-CN,zh;q=0.9",
	}

	chromeHeaderPriority = http2.PriorityParam{
		StreamDep: 0,
		Exclusive: true,
		Weight:    255,
	}
)

// ImpersonateChrome impersonates Chrome browser (version 120).
func (c *Client) ImpersonateChrome() *Client {
	c.
		SetTLSFingerprint(utls.HelloChrome_120).
		SetHTTP2SettingsFrame(chromeHttp2Settings...).
		SetHTTP2ConnectionFlow(15663105).
		SetCommonPseudoHeaderOrder(chromePseudoHeaderOrder...).
		SetCommonHeaderOrder(chromeHeaderOrder...).
		SetCommonHeaders(chromeHeaders).
		SetHTTP2HeaderPriority(chromeHeaderPriority).
		SetMultipartBoundaryFunc(webkitMultipartBoundaryFunc)
	return c
}

// ImpersonateCustomChrome impersonates a given Chrome fingerprint.
// We replace any common headers with actual headers from the client if available.
func (c *Client) ImpersonateCustomChrome(hdrs http.Header, rawClientHello []byte) *Client {
	commonHeaders := mergeHeaders(chromeHeaders, hdrs)
	c.
		SetCustomTLSFingerprint(rawClientHello).
		SetHTTP2SettingsFrame(chromeCustomHttp2Settings...).
		SetHTTP2ConnectionFlow(15663105).
		SetCommonPseudoHeaderOrder(chromePseudoHeaderOrder...).
		SetCommonHeaderOrder(chromeHeaderOrder...).
		SetCommonHeaders(commonHeaders).
		SetHTTP2HeaderPriority(chromeHeaderPriority).
		SetMultipartBoundaryFunc(webkitMultipartBoundaryFunc)
	return c
}

var (
	firefoxHttp2Settings = []http2.Setting{
		{
			ID:  http2.SettingHeaderTableSize,
			Val: 65536,
		},
		{
			ID:  http2.SettingEnablePush,
			Val: 0,
		},
		{
			ID:  http2.SettingInitialWindowSize,
			Val: 131072,
		},
		{
			ID:  http2.SettingMaxFrameSize,
			Val: 16384,
		},
	}

	firefoxPriorityFrames = []http2.PriorityFrame{
		{
			StreamID: 3,
			PriorityParam: http2.PriorityParam{
				StreamDep: 0,
				Exclusive: false,
				Weight:    200,
			},
		},
		{
			StreamID: 5,
			PriorityParam: http2.PriorityParam{
				StreamDep: 0,
				Exclusive: false,
				Weight:    100,
			},
		},
		{
			StreamID: 7,
			PriorityParam: http2.PriorityParam{
				StreamDep: 0,
				Exclusive: false,
				Weight:    0,
			},
		},
		{
			StreamID: 9,
			PriorityParam: http2.PriorityParam{
				StreamDep: 7,
				Exclusive: false,
				Weight:    0,
			},
		},
		{
			StreamID: 11,
			PriorityParam: http2.PriorityParam{
				StreamDep: 3,
				Exclusive: false,
				Weight:    0,
			},
		},
		{
			StreamID: 13,
			PriorityParam: http2.PriorityParam{
				StreamDep: 0,
				Exclusive: false,
				Weight:    240,
			},
		},
	}

	firefoxPseudoHeaderOrder = []string{
		":method",
		":path",
		":authority",
		":scheme",
	}

	firefoxHeaderOrder = []string{
		"user-agent",
		"accept",
		"accept-language",
		"accept-encoding",
		"referer",
		"cookie",
		"upgrade-insecure-requests",
		"sec-fetch-dest",
		"sec-fetch-mode",
		"sec-fetch-site",
		"sec-fetch-user",
		"te",
	}

	firefoxHeaders = map[string]string{
		"user-agent":                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:120.0) Gecko/20100101 Firefox/120.0",
		"accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
		"accept-language":           "zh-CN,zh;q=0.8,zh-TW;q=0.7,zh-HK;q=0.5,en-US;q=0.3,en;q=0.2",
		"upgrade-insecure-requests": "1",
		"sec-fetch-dest":            "document",
		"sec-fetch-mode":            "navigate",
		"sec-fetch-site":            "same-origin",
		"sec-fetch-user":            "?1",
		//"te":                        "trailers",
	}

	firefoxHeaderPriority = http2.PriorityParam{
		StreamDep: 13,
		Exclusive: false,
		Weight:    41,
	}
)

// ImpersonateFirefox impersonates Firefox browser (version 120).
func (c *Client) ImpersonateFirefox() *Client {
	c.
		SetTLSFingerprint(utls.HelloFirefox_120).
		SetHTTP2SettingsFrame(firefoxHttp2Settings...).
		SetHTTP2ConnectionFlow(12517377).
		SetHTTP2PriorityFrames(firefoxPriorityFrames...).
		SetCommonPseudoHeaderOrder(firefoxPseudoHeaderOrder...).
		SetCommonHeaderOrder(firefoxHeaderOrder...).
		SetCommonHeaders(firefoxHeaders).
		SetHTTP2HeaderPriority(firefoxHeaderPriority).
		SetMultipartBoundaryFunc(firefoxMultipartBoundaryFunc)
	return c
}

// ImpersonateCustomFirefox impersonates a given Firefox fingerprint.
// We replace any common headers with actual headers from the client if available.
func (c *Client) ImpersonateCustomFirefox(hdrs http.Header, rawClientHello []byte) *Client {
	commonHeaders := mergeHeaders(firefoxHeaders, hdrs)
	c.
		SetCustomTLSFingerprint(rawClientHello).
		SetHTTP2SettingsFrame(firefoxHttp2Settings...).
		SetHTTP2ConnectionFlow(12517377).
		SetHTTP2PriorityFrames(firefoxPriorityFrames...).
		SetCommonPseudoHeaderOrder(firefoxPseudoHeaderOrder...).
		SetCommonHeaderOrder(firefoxHeaderOrder...).
		SetCommonHeaders(commonHeaders).
		SetHTTP2HeaderPriority(firefoxHeaderPriority).
		SetMultipartBoundaryFunc(firefoxMultipartBoundaryFunc)
	return c
}

var (
	safariHttp2Settings = []http2.Setting{
		{
			ID:  http2.SettingInitialWindowSize,
			Val: 4194304,
		},
		{
			ID:  http2.SettingMaxConcurrentStreams,
			Val: 100,
		},
	}

	safariPseudoHeaderOrder = []string{
		":method",
		":scheme",
		":path",
		":authority",
	}

	safariHeaderOrder = []string{
		"accept",
		"sec-fetch-site",
		"cookie",
		"sec-fetch-dest",
		"accept-language",
		"sec-fetch-mode",
		"user-agent",
		"referer",
		"accept-encoding",
	}

	safariHeaders = map[string]string{
		"accept":          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
		"sec-fetch-site":  "same-origin",
		"sec-fetch-dest":  "document",
		"accept-language": "zh-CN,zh-Hans;q=0.9",
		"sec-fetch-mode":  "navigate",
		"user-agent":      "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.6 Safari/605.1.15",
	}

	safariHeaderPriority = http2.PriorityParam{
		StreamDep: 0,
		Exclusive: false,
		Weight:    254,
	}
)

// ImpersonateSafari impersonates Safari browser (version 16.6).
func (c *Client) ImpersonateSafari() *Client {
	c.
		SetTLSFingerprint(utls.HelloSafari_16_0).
		SetHTTP2SettingsFrame(safariHttp2Settings...).
		SetHTTP2ConnectionFlow(10485760).
		SetCommonPseudoHeaderOrder(safariPseudoHeaderOrder...).
		SetCommonHeaderOrder(safariHeaderOrder...).
		SetCommonHeaders(safariHeaders).
		SetHTTP2HeaderPriority(safariHeaderPriority).
		SetMultipartBoundaryFunc(webkitMultipartBoundaryFunc)
	return c
}

// ImpersonateCustomSafari impersonates a given Safari fingerprint.
// We replace any common headers with actual headers from the client if available.
func (c *Client) ImpersonateCustomSafari(hdrs http.Header, rawClientHello []byte) *Client {
	commonHeaders := mergeHeaders(safariHeaders, hdrs)
	c.
		SetTLSFingerprint(utls.HelloSafari_16_0).
		SetHTTP2SettingsFrame(safariHttp2Settings...).
		SetHTTP2ConnectionFlow(10485760).
		SetCommonPseudoHeaderOrder(safariPseudoHeaderOrder...).
		SetCommonHeaderOrder(safariHeaderOrder...).
		SetCommonHeaders(commonHeaders).
		SetHTTP2HeaderPriority(safariHeaderPriority).
		SetMultipartBoundaryFunc(webkitMultipartBoundaryFunc)
	return c
}

// mergeHeaders is a helper function to merge expected with actual browser headers.
func mergeHeaders(common map[string]string, actual http.Header) map[string]string {
	headers := make(map[string]string)
	maps.Copy(headers, common)
	for k := range actual {
		if v := actual.Get(k); v != "" {
			headers[k] = v
		}
	}
	return headers
}
