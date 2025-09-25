package main

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/elazarl/goproxy"
	"github.com/wwengg/douyin/fay/impl"
	"github.com/wwengg/douyin/model"
	"github.com/wwengg/douyin/proto"
	"github.com/wwengg/douyin/utils"
)

func main() {
	utils.ConfigureCA()
	proxy := goproxy.NewProxyHttpServer()
	proxy.Verbose = false

	fayProxyServer := impl.NewFayProxyServer()
	go fayProxyServer.StartWebsocket()

	//ws数据处理
	proxy.AddWebsocketHandler(func(data []byte, direction goproxy.WebsocketDirection, ctx *goproxy.ProxyCtx) (reply []byte) {
		reply = data
		if len(data) == 0 {
			return
		}
		if data[0] != 0x08 {
			return
		}
		wssResponse := proto.WssResponse{}
		if err := wssResponse.XXX_Unmarshal(data); err == nil {
			//检测包格式
			if v, ok := wssResponse.Headers["compress_type"]; !ok && v != "gzip" {
				return
			}
			//解压gzip
			deData, err := utils.GzipDecode(wssResponse.Payload)
			if err != nil {
				ctx.Logf("gzip解压失败")
				return
			}
			res := proto.Response{}
			if err = res.XXX_Unmarshal(deData); err != nil {
				return
			}
			for _, message := range res.Messages {
				fayProxyServer.DoMessage(message)
			}
		}
		return
	})

	proxy.WebSocketHandler = func(dst io.Writer, src io.Reader, direction goproxy.WebsocketDirection, ctx *goproxy.ProxyCtx) error {
		fullPacket := make([]byte, 0)
		buf := make([]byte, 32*1024)
		var err error = nil
		for {
			nr, er := src.Read(buf)

			if er != nil {
				if er != io.EOF {
					err = er
				}
				break
			}

			if nr > 0 {
				fullPacket = append(fullPacket, buf[:nr]...)
				websocketPacket := model.NewWebsocketPacket(fullPacket)

				if !websocketPacket.Valid {
					continue
				}

				websocketPacket.Payload = proxy.FilterWebsocketPacket(websocketPacket.Payload, direction, ctx)
				encodedPacket := websocketPacket.Encode()
				nw, ew := dst.Write(encodedPacket)
				fullPacket = fullPacket[websocketPacket.PacketSize:]

				if nw < 0 || len(encodedPacket) < nw {
					nw = 0
					if ew == nil {
						ew = errors.New("invalid write result")
					}
				}
				if ew != nil {
					err = ew
					break
				}
				if len(encodedPacket) != nw {
					err = io.ErrShortWrite
					break
				}
			}
		}
		return err
	}

	proxy.OnRequest(goproxy.ReqHostMatches(regexp.MustCompile(".*(douyin|amemv)\\.com:443$"))).
		HandleConnect(goproxy.AlwaysMitm)

	streamExtractor := NewStreamExtractor()

	proxy.OnResponse().DoFunc(
		func(resp *http.Response, ctx *goproxy.ProxyCtx) *http.Response {
			if resp == nil || resp.Body == nil || resp.Request == nil || resp.Request.URL == nil {
				return resp
			}

			host := resp.Request.URL.Host
			if !strings.Contains(host, "douyin.com") && !strings.Contains(host, "amemv.com") {
				return resp
			}

			rawBody, err := io.ReadAll(resp.Body)
			if err != nil {
				resp.Body = io.NopCloser(bytes.NewBuffer(rawBody))
				return resp
			}

			resp.Body = io.NopCloser(bytes.NewBuffer(rawBody))

			body := rawBody
			if strings.Contains(resp.Header.Get("Content-Encoding"), "gzip") {
				gzipReader, gzipErr := gzip.NewReader(bytes.NewReader(rawBody))
				if gzipErr == nil {
					defer gzipReader.Close()
					if decoded, decodeErr := io.ReadAll(gzipReader); decodeErr == nil {
						body = decoded
					}
				}
			}

			streamExtractor.ExtractAndLog(body)
			return resp
		},
	)
	log.Println("软件准备就绪，请启动【直播伴侣】并且点击【开始直播】")
	log.Fatal(http.ListenAndServe(":8001", proxy))
}

type RtmpLive struct {
	Data struct {
		StreamUrl struct {
			RtmpPushUrl string `json:"rtmp_push_url"`
		} `json:"stream_url"`
	} `json:"data"`
}

type StreamExtractor struct {
	lastURL string
	mu      sync.Mutex
	regex   *regexp.Regexp
}

func NewStreamExtractor() *StreamExtractor {
	return &StreamExtractor{
		regex: regexp.MustCompile(`rtmp://[^"\\s]+`),
	}
}

func (s *StreamExtractor) ExtractAndLog(body []byte) {
	if len(body) == 0 {
		return
	}

	var payload interface{}
	if err := json.Unmarshal(body, &payload); err == nil {
		s.extractFromInterface(payload)
		return
	}

	matches := s.regex.FindAllString(string(body), -1)
	s.logMatches(matches)
}

func (s *StreamExtractor) extractFromInterface(node interface{}) {
	switch value := node.(type) {
	case map[string]interface{}:
		for _, v := range value {
			s.extractFromInterface(v)
		}
	case []interface{}:
		for _, v := range value {
			s.extractFromInterface(v)
		}
	case string:
		s.logMatches([]string{value})
	}
}

func (s *StreamExtractor) logMatches(matches []string) {
	for _, match := range matches {
		if !strings.HasPrefix(match, "rtmp://") {
			continue
		}
		s.logStreamURL(match)
	}
}

func (s *StreamExtractor) logStreamURL(rtmpURL string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rtmpURL == "" || rtmpURL == s.lastURL {
		return
	}
	s.lastURL = rtmpURL

	parsed, err := url.Parse(rtmpURL)
	if err != nil {
		log.Printf("检测到推流地址：%s", rtmpURL)
		return
	}

	serverPath, streamKey := path.Split(parsed.Path)
	if streamKey == "" {
		streamKey = strings.TrimPrefix(serverPath, "/")
		serverPath = "/"
	}

	if parsed.RawQuery != "" {
		streamKey = fmt.Sprintf("%s?%s", streamKey, parsed.RawQuery)
	}

	server := fmt.Sprintf("%s://%s%s", parsed.Scheme, parsed.Host, serverPath)
	log.Printf("服务器：%s", server)
	log.Printf("推流码：%s", streamKey)
}
