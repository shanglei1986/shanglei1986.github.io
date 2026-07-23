package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	fhttp "github.com/bogdanfinn/fhttp"
	tls_client "github.com/bogdanfinn/tls-client"
	"github.com/bogdanfinn/tls-client/profiles"
	"github.com/chromedp/cdproto/fetch"
	"github.com/chromedp/cdproto/network"
	"github.com/chromedp/chromedp"
)

// ============ 端口自行配置 ============

type Config struct {
	Port            int
	MaxConcurrent   int
	ExtractTimeout  time.Duration
	FetchTimeout    time.Duration
	CacheTTL        time.Duration
	CleanupInterval time.Duration
	BrowserIdleTime time.Duration
	Verbose         bool
	EmbedDomain     string // 默认 embed 域名，可通过 EMBED_DOMAIN 环境变量设置
	StreamsFile     string // 节目列表 JSON；仍可通过 STREAMS_FILE 环境变量覆盖
}

var config = Config{
	Port:            11458,   //更改为自己端口
	MaxConcurrent:   5,
	ExtractTimeout:  20 * time.Second,
	FetchTimeout:    10 * time.Second,
	CacheTTL:        5 * time.Minute,
	CleanupInterval: 1 * time.Minute,
	BrowserIdleTime: 5 * time.Minute,
	Verbose:         false,
	EmbedDomain:     "embedindia.st",
	StreamsFile:     "/docker-compose-files/File_server/IPTVAgentCode/ppv_live_go/streams.json",
}

// ============ 全局状态 ============

type CacheEntry struct {
	URL         string
	EmbedOrigin string // 记录提取时使用的 embed 来源
	Time        time.Time
}

var (
	m3u8Cache     = make(map[string]*CacheEntry)
	cacheMu       sync.RWMutex
	activePages   int32
	activePagesMu sync.Mutex
	totalRequests  atomic.Int64
	browserCtx     context.Context
	browserCancel  context.CancelFunc
	browserMu      sync.Mutex
	browserLastUse time.Time
	tlsClient      tls_client.HttpClient
	tlsClientMu    sync.Mutex

	// 记录最近使用的 embed origin，供状态接口观察。
	lastEmbedOrigin   string = "https://embedindia.st"
	lastEmbedOriginMu sync.RWMutex

	streamIndex         = make(map[string]string)
	streamIndexMu       sync.RWMutex
	streamIndexPath     string
	streamIndexModTime  time.Time
	streamIndexFileSize int64
)

func logMsg(format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	log.Printf("[%s] %s", time.Now().Format("2006-01-02 15:04:05"), msg)
}

func logVerbose(format string, args ...interface{}) {
	if config.Verbose {
		logMsg(format, args...)
	}
}

func normalizeOrigin(raw string) string {
	parsed, err := url.Parse(raw)
	if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return ""
	}
	return parsed.Scheme + "://" + parsed.Host
}

func defaultEmbedOrigin() string {
	raw := strings.TrimSpace(strings.TrimSuffix(config.EmbedDomain, "/"))
	if strings.Contains(raw, "://") {
		if origin := normalizeOrigin(raw); origin != "" {
			return origin
		}
	}
	return "https://" + raw
}

func updateLastEmbedOrigin(origin string) {
	if origin == "" {
		return
	}
	lastEmbedOriginMu.Lock()
	lastEmbedOrigin = origin
	lastEmbedOriginMu.Unlock()
}

// ============ streams.json 动态解析 ============

type streamListFile struct {
	Streams []streamGroup `json:"streams"`
}

type streamGroup struct {
	Streams []streamItem `json:"streams"`
}

type streamItem struct {
	URIName    string       `json:"uri_name"`
	Iframe     string       `json:"iframe"`
	Substreams []streamItem `json:"substreams"`
}

func indexStreamItem(index map[string]string, item streamItem) {
	if item.URIName != "" && normalizeOrigin(item.Iframe) != "" {
		index[item.URIName] = item.Iframe
	}
	for _, substream := range item.Substreams {
		indexStreamItem(index, substream)
	}
}

func locateStreamsFile() string {
	if config.StreamsFile == "" {
		return ""
	}

	candidates := []string{config.StreamsFile}
	if !filepath.IsAbs(config.StreamsFile) {
		if executable, err := os.Executable(); err == nil {
			candidates = append(candidates, filepath.Join(filepath.Dir(executable), config.StreamsFile))
		}
	}

	seen := make(map[string]bool)
	for _, candidate := range candidates {
		absolute, err := filepath.Abs(candidate)
		if err != nil || seen[absolute] {
			continue
		}
		seen[absolute] = true
		if info, err := os.Stat(absolute); err == nil && !info.IsDir() {
			return absolute
		}
	}
	return ""
}

func reloadStreamIndex(force bool) error {
	path := locateStreamsFile()
	if path == "" {
		if force {
			return fmt.Errorf("%s not found", config.StreamsFile)
		}
		return nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return err
	}

	streamIndexMu.RLock()
	unchanged := path == streamIndexPath &&
		info.ModTime().Equal(streamIndexModTime) &&
		info.Size() == streamIndexFileSize
	streamIndexMu.RUnlock()
	if !force && unchanged {
		return nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}

	var list streamListFile
	if err := json.Unmarshal(data, &list); err != nil {
		return fmt.Errorf("parse %s: %w", path, err)
	}

	index := make(map[string]string)
	for _, group := range list.Streams {
		for _, item := range group.Streams {
			indexStreamItem(index, item)
		}
	}

	streamIndexMu.Lock()
	streamIndex = index
	streamIndexPath = path
	streamIndexModTime = info.ModTime()
	streamIndexFileSize = info.Size()
	streamIndexMu.Unlock()

	logMsg("✓ 已载入节目 iframe: %d 条 (%s)", len(index), path)
	return nil
}

func iframeForURI(uri string) string {
	if err := reloadStreamIndex(false); err != nil {
		logVerbose("刷新 streams.json 失败: %s", err)
	}
	streamIndexMu.RLock()
	iframe := streamIndex[uri]
	streamIndexMu.RUnlock()
	return iframe
}

// ============ 输入解析 ============

// parseInput 统一处理 uri= 和 url= 参数
// 返回: cacheKey, embedURL, embedOrigin
func parseInput(r *http.Request) (string, string, string, error) {
	uri := r.URL.Query().Get("uri")
	rawURL := r.URL.Query().Get("url")

	if uri == "" && rawURL == "" {
		return "", "", "", fmt.Errorf("missing uri or url parameter")
	}

	if rawURL != "" {
		// 完整 URL 模式: url=https://xxxx.xx/embed/xxxxx
		parsed, err := url.Parse(rawURL)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return "", "", "", fmt.Errorf("invalid embed url")
		}

		embedOrigin := parsed.Scheme + "://" + parsed.Host
		updateLastEmbedOrigin(embedOrigin)
		return rawURL, rawURL, embedOrigin, nil
	}

	// uri 模式优先使用 streams.json 中该节目的完整 iframe。
	uri = strings.TrimPrefix(uri, "/")
	embedURL := iframeForURI(uri)
	if embedURL == "" {
		embedURL = fmt.Sprintf("%s/embed/%s", defaultEmbedOrigin(), uri)
	}
	embedOrigin := normalizeOrigin(embedURL)
	if embedOrigin == "" {
		return "", "", "", fmt.Errorf("invalid iframe for uri %s", uri)
	}

	updateLastEmbedOrigin(embedOrigin)
	// 完整 iframe URL 作为缓存键，域名变化后不会误用旧域名的缓存。
	return embedURL, embedURL, embedOrigin, nil
}

// ============ TLS Client ============

func getTLSClient() (tls_client.HttpClient, error) {
	tlsClientMu.Lock()
	defer tlsClientMu.Unlock()

	if tlsClient != nil {
		return tlsClient, nil
	}

	jar := tls_client.NewCookieJar()
	options := []tls_client.HttpClientOption{
		tls_client.WithTimeoutSeconds(int(config.FetchTimeout.Seconds())),
		tls_client.WithClientProfile(profiles.Chrome_120),
		tls_client.WithNotFollowRedirects(),
		tls_client.WithCookieJar(jar),
		tls_client.WithInsecureSkipVerify(),
	}

	client, err := tls_client.NewHttpClient(tls_client.NewNoopLogger(), options...)
	if err != nil {
		return nil, err
	}

	tlsClient = client
	return tlsClient, nil
}

func resetTLSClient() {
	tlsClientMu.Lock()
	defer tlsClientMu.Unlock()
	tlsClient = nil
}

// ============ 浏览器管理 ============

func initBrowser() error {
	browserMu.Lock()
	defer browserMu.Unlock()

	if browserCtx != nil {
		return nil
	}

	logMsg("启动浏览器...")

	opts := append(chromedp.DefaultExecAllocatorOptions[:],
		chromedp.Flag("headless", true),
		chromedp.Flag("no-sandbox", true),
		chromedp.Flag("disable-setuid-sandbox", true),
		chromedp.Flag("disable-dev-shm-usage", true),
		chromedp.Flag("disable-gpu", true),
		chromedp.Flag("disable-extensions", true),
		chromedp.Flag("disable-background-networking", true),
		chromedp.UserAgent("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"),
	)

	allocCtx, allocCancel := chromedp.NewExecAllocator(context.Background(), opts...)
	ctx, cancel := chromedp.NewContext(allocCtx,
		chromedp.WithLogf(func(string, ...interface{}) {}),
		chromedp.WithErrorf(func(string, ...interface{}) {}),
	)

	if err := chromedp.Run(ctx); err != nil {
		allocCancel()
		cancel()
		return fmt.Errorf("启动浏览器失败: %w", err)
	}

	browserCtx = ctx
	browserCancel = func() {
		cancel()
		allocCancel()
	}
	browserLastUse = time.Now()

	logMsg("✓ 浏览器已就绪")
	return nil
}

func closeBrowserLocked() {
	if browserCancel != nil {
		logMsg("关闭浏览器...")
		browserCancel()
		browserCtx = nil
		browserCancel = nil
		browserLastUse = time.Time{}
	}
}

func closeBrowser() {
	browserMu.Lock()
	defer browserMu.Unlock()
	closeBrowserLocked()
}

func restartBrowser() error {
	closeBrowser()
	return initBrowser()
}

func touchBrowser() {
	browserMu.Lock()
	if browserCtx != nil {
		browserLastUse = time.Now()
	}
	browserMu.Unlock()
}

func browserIdleLoop() {
	if config.BrowserIdleTime <= 0 {
		return
	}

	checkEvery := config.BrowserIdleTime / 2
	if checkEvery < 30*time.Second {
		checkEvery = 30 * time.Second
	}
	if checkEvery > time.Minute {
		checkEvery = time.Minute
	}

	ticker := time.NewTicker(checkEvery)
	defer ticker.Stop()

	for range ticker.C {
		activePagesMu.Lock()
		pages := activePages
		activePagesMu.Unlock()
		if pages != 0 {
			continue
		}

		browserMu.Lock()
		if browserCtx != nil && !browserLastUse.IsZero() &&
			time.Since(browserLastUse) >= config.BrowserIdleTime {
			logMsg("浏览器已空闲 %s，自动释放", config.BrowserIdleTime)
			closeBrowserLocked()
		}
		browserMu.Unlock()
	}
}

// ============ 提取 m3u8 地址 ============

func extractM3u8(cacheKey, embedURL, embedOrigin string) (string, bool, error) {
	// 检查缓存
	cacheMu.RLock()
	entry, cacheHit := m3u8Cache[cacheKey]
	if cacheHit && time.Since(entry.Time) >= config.CacheTTL {
		cacheHit = false
	}
	cacheMu.RUnlock()
	if cacheHit {
		// m3u8 地址常带短期 token；命中缓存后先验证，失效就立即重新提取。
		body, status, err := fetchM3u8Content(entry.URL, entry.EmbedOrigin)
		if err == nil && status == http.StatusOK && isM3u8Content(body) {
			logVerbose("缓存命中且有效: %s", cacheKey)
			return entry.URL, true, nil
		}

		cacheMu.Lock()
		if current := m3u8Cache[cacheKey]; current == entry {
			delete(m3u8Cache, cacheKey)
		}
		cacheMu.Unlock()
		logMsg("缓存已失效，重新提取: %s (status=%d err=%v)", cacheKey, status, err)
	}

	// 并发控制
	activePagesMu.Lock()
	if int(activePages) >= config.MaxConcurrent {
		activePagesMu.Unlock()
		return "", false, fmt.Errorf("too many concurrent requests")
	}
	activePages++
	activePagesMu.Unlock()
	defer func() {
		activePagesMu.Lock()
		activePages--
		activePagesMu.Unlock()
	}()

	// 确保浏览器就绪
	if err := initBrowser(); err != nil {
		return "", false, err
	}

	browserMu.Lock()
	ctx := browserCtx
	browserLastUse = time.Now()
	browserMu.Unlock()

	if ctx == nil {
		return "", false, fmt.Errorf("browser not available")
	}

	// 创建新 tab
	tabCtx, tabCancel := chromedp.NewContext(ctx)
	defer func() {
		tabCancel()
		touchBrowser()
	}()

	timeoutCtx, timeoutCancel := context.WithTimeout(tabCtx, config.ExtractTimeout)
	defer timeoutCancel()

	logVerbose("访问: %s", embedURL)

	var m3u8URL string
	m3u8Found := make(chan string, 1)

	// 监听网络请求 - 动态匹配任何 .m3u8 链接
	chromedp.ListenTarget(timeoutCtx, func(ev interface{}) {
		switch e := ev.(type) {
		case *network.EventResponseReceived:
			u := e.Response.URL
			if isM3u8URL(u) && !strings.Contains(u, "/embed/") {
				select {
				case m3u8Found <- u:
				default:
				}
			}
		case *fetch.EventRequestPaused:
			go func() {
				resType := e.ResourceType
				if resType == network.ResourceTypeImage ||
					resType == network.ResourceTypeFont ||
					resType == network.ResourceTypeStylesheet ||
					resType == network.ResourceTypeMedia {
					_ = chromedp.Run(timeoutCtx, fetch.FailRequest(e.RequestID, network.ErrorReasonBlockedByClient))
				} else {
					_ = chromedp.Run(timeoutCtx, fetch.ContinueRequest(e.RequestID))
				}
			}()
		}
	})

	// 启用网络监听和请求拦截
	err := chromedp.Run(timeoutCtx,
		network.Enable(),
		fetch.Enable().WithPatterns([]*fetch.RequestPattern{
			{URLPattern: "*", RequestStage: fetch.RequestStageRequest},
		}),
		chromedp.Navigate(embedURL),
	)
	if err != nil {
		if strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "target closed") {
			_ = restartBrowser()
		}
		return "", false, fmt.Errorf("navigate failed: %w", err)
	}

	// 等待 m3u8 URL
	select {
	case u := <-m3u8Found:
		m3u8URL = u
	case <-time.After(config.ExtractTimeout - 2*time.Second):
		var videoSrc string
		_ = chromedp.Run(timeoutCtx, chromedp.Evaluate(`
			(function() {
				var v = document.querySelector('video');
				if (v && v.src && v.src.includes('m3u8')) return v.src;
				var s = document.querySelector('video source');
				if (s && s.src && s.src.includes('m3u8')) return s.src;
				return '';
			})()
		`, &videoSrc))
		if videoSrc != "" {
			m3u8URL = videoSrc
		}
	}

	if m3u8URL == "" {
		return "", false, fmt.Errorf("m3u8 not found")
	}

	// 缓存（包含 embed origin）
	cacheMu.Lock()
	m3u8Cache[cacheKey] = &CacheEntry{
		URL:         m3u8URL,
		EmbedOrigin: embedOrigin,
		Time:        time.Now(),
	}
	cacheMu.Unlock()

	logMsg("提取成功: %s → %s", cacheKey, m3u8URL)
	return m3u8URL, false, nil
}

// isM3u8URL 检查 URL 是否为 m3u8 文件
func isM3u8URL(u string) bool {
	if idx := strings.Index(u, "?"); idx > 0 {
		u = u[:idx]
	}
	return strings.HasSuffix(u, ".m3u8")
}

func isM3u8Content(text string) bool {
	text = strings.TrimPrefix(text, "\uFEFF")
	return strings.HasPrefix(strings.TrimSpace(text), "#EXTM3U")
}

// ============ 获取 m3u8 内容 ============

func fetchM3u8Content(targetURL, embedOrigin string) (string, int, error) {
	client, err := getTLSClient()
	if err != nil {
		return "", 500, fmt.Errorf("TLS client error: %w", err)
	}

	req, err := fhttp.NewRequest(fhttp.MethodGet, targetURL, nil)
	if err != nil {
		return "", 500, err
	}

	if embedOrigin == "" {
		embedOrigin = defaultEmbedOrigin()
	}
	req.Header.Set("Referer", embedOrigin+"/")
	req.Header.Set("Origin", embedOrigin)
	req.Header.Set("Accept", "*/*")

	resp, err := client.Do(req)
	if err != nil {
		logMsg("TLS请求失败，重置client: %s", err)
		resetTLSClient()
		return "", 502, fmt.Errorf("fetch failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", 502, err
	}

	return string(body), resp.StatusCode, nil
}

// ============ 改写 m3u8 ============

var (
	// 匹配绝对路径 m3u8 URL (任意域名)
	reAbsM3u8 = regexp.MustCompile(`(?m)^(https?://[^\s]+\.m3u8[^\s]*)$`)
	// 匹配相对路径 m3u8
	reRelM3u8 = regexp.MustCompile(`(?m)^([a-zA-Z0-9_\-\./]+\.m3u8[^\s]*)$`)
	// 匹配所有绝对 URL（用于 ts 代理）
	reAbsURL = regexp.MustCompile(`(?m)^(https?://[^\s]+)$`)
	// 匹配相对路径的 ts/segment（非 m3u8，非注释，非绝对URL）
	reRelSegment = regexp.MustCompile(`(?m)^([a-zA-Z0-9_\-\.][a-zA-Z0-9_\-\./]*\.[^\s]+)$`)
)

const originQueryParam = "__modi_origin"

func addProxyParams(proxyURL, embedOrigin string, proxyTs bool) string {
	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return proxyURL
	}
	query := parsed.Query()
	if embedOrigin != "" {
		query.Set(originQueryParam, embedOrigin)
	}
	if proxyTs {
		query.Set("mode", "proxy")
	}
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func rewriteM3u8(text, targetURL, baseURL, embedOrigin string, proxyTs bool) string {
	u, err := url.Parse(targetURL)
	if err != nil {
		return text
	}

	targetHost := u.Host
	pathParts := strings.Split(u.Path, "/")
	if len(pathParts) > 0 {
		pathParts = pathParts[:len(pathParts)-1]
	}
	basePath := strings.Join(pathParts, "/")

	// 1. 改写绝对路径 m3u8 URL
	text = reAbsM3u8.ReplaceAllStringFunc(text, func(match string) string {
		trimmed := strings.TrimSpace(match)
		parsed, err := url.Parse(trimmed)
		if err != nil {
			return match
		}
		proxyPath := fmt.Sprintf("%s/proxy/%s%s", baseURL, parsed.Host, parsed.RequestURI())
		return addProxyParams(proxyPath, embedOrigin, proxyTs)
	})

	// 2. 改写相对路径 m3u8
	text = reRelM3u8.ReplaceAllStringFunc(text, func(match string) string {
		trimmed := strings.TrimSpace(match)
		proxyPath := fmt.Sprintf("%s/proxy/%s%s/%s", baseURL, targetHost, basePath, trimmed)
		return addProxyParams(proxyPath, embedOrigin, proxyTs)
	})

	// 3. 改写 ts 切片绝对 URL（仅 proxy 模式）
	if proxyTs {
		text = reAbsURL.ReplaceAllStringFunc(text, func(match string) string {
			trimmed := strings.TrimSpace(match)
			// 跳过已改写的 URL
			if strings.Contains(trimmed, "/proxy/") || strings.Contains(trimmed, "/ts/") {
				return match
			}
			// 跳过 m3u8 URL（已在上面处理过）
			if isM3u8URL(trimmed) {
				return match
			}
			parsed, err := url.Parse(trimmed)
			if err != nil {
				return match
			}
			proxyPath := fmt.Sprintf("%s/ts/%s%s", baseURL, parsed.Host, parsed.RequestURI())
			return addProxyParams(proxyPath, embedOrigin, false)
		})
	}

	// 4. 改写相对路径的 ts/segment（非 m3u8 的相对路径）
	//    proxyTs=false: 改成直连原始CDN的绝对URL，避免经过反代
	//    proxyTs=true:  改成经 /ts/ 代理的URL
	text = reRelSegment.ReplaceAllStringFunc(text, func(match string) string {
		trimmed := strings.TrimSpace(match)
		// 跳过 m3u8（已在上面处理过）
		if isM3u8URL(trimmed) {
			return match
		}
		// 跳过已改写的
		if strings.Contains(trimmed, "/proxy/") || strings.Contains(trimmed, "/ts/") {
			return match
		}
		// 跳过 #EXT 注释行（安全起见）
		if strings.HasPrefix(trimmed, "#") {
			return match
		}
		if proxyTs {
			proxyPath := fmt.Sprintf("%s/ts/%s%s/%s", baseURL, targetHost, basePath, trimmed)
			return addProxyParams(proxyPath, embedOrigin, false)
		}
		// 直连CDN: 拼成绝对URL
		return fmt.Sprintf("https://%s%s/%s", targetHost, basePath, trimmed)
	})

	return text
}

// ============ 缓存清理 ============

func cacheCleanupLoop() {
	ticker := time.NewTicker(config.CleanupInterval)
	defer ticker.Stop()

	for range ticker.C {
		now := time.Now()
		cacheMu.Lock()
		expired := 0
		for k, v := range m3u8Cache {
			if now.Sub(v.Time) > config.CacheTTL {
				delete(m3u8Cache, k)
				expired++
			}
		}
		remaining := len(m3u8Cache)
		cacheMu.Unlock()
		if expired > 0 {
			logVerbose("清理 %d 条过期缓存，剩余 %d", expired, remaining)
		}
	}
}

// ============ HTTP 处理 ============

func getBaseURL(r *http.Request) string {
	scheme := r.Header.Get("X-Forwarded-Proto")
	if scheme == "" {
		scheme = "http"
	}
	host := r.Header.Get("X-Forwarded-Host")
	if host == "" {
		host = r.Header.Get("Host")
	}
	if host == "" {
		host = r.Host
	}
	return scheme + "://" + host
}

func jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	json.NewEncoder(w).Encode(data)
}

func m3u8Response(w http.ResponseWriter, text string) {
	w.Header().Set("Content-Type", "application/vnd.apple.mpegurl")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")
	w.Write([]byte(text))
}

func handleM3u8(w http.ResponseWriter, r *http.Request) {
	cacheKey, embedURL, embedOrigin, err := parseInput(r)
	if err != nil {
		jsonResponse(w, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}

	logVerbose("获取m3u8: %s (embed: %s)", cacheKey, embedURL)
	m3u8URL, cached, err := extractM3u8(cacheKey, embedURL, embedOrigin)
	if err != nil {
		logMsg("获取失败 %s: %s", cacheKey, err)
		jsonResponse(w, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}

	logVerbose("m3u8结果: %s(缓存=%v)", cacheKey, cached)
	jsonResponse(w, map[string]interface{}{
		"success": true,
		"m3u8":    m3u8URL,
		"cached":  cached,
	})
}

func handleProxy(w http.ResponseWriter, r *http.Request) {
	// 解析 /proxy/{host}/{path}
	path := strings.TrimPrefix(r.URL.Path, "/proxy/")
	slashIdx := strings.Index(path, "/")
	if slashIdx < 0 {
		http.Error(w, "Invalid proxy path", 400)
		return
	}

	host := path[:slashIdx]
	remotePath := path[slashIdx:]
	targetURL := fmt.Sprintf("https://%s%s", host, remotePath)

	query := r.URL.Query()
	embedOrigin := normalizeOrigin(query.Get(originQueryParam))
	query.Del(originQueryParam)
	query.Del("mode")

	// 保留原始目标 query 参数（排除代理自身参数）
	if rawQuery := r.URL.RawQuery; rawQuery != "" {
		if remaining := query.Encode(); remaining != "" {
			if strings.Contains(targetURL, "?") {
				targetURL += "&" + remaining
			} else {
				targetURL += "?" + remaining
			}
		}
	}

	logVerbose("代理m3u8: %s", targetURL)

	text, status, err := fetchM3u8Content(targetURL, embedOrigin)
	if err != nil || status != 200 {
		logMsg("代理失败: status=%d err=%v", status, err)
		if status == 0 {
			status = 502
		}
		http.Error(w, "Proxy failed", status)
		return
	}

	proxyTs := r.URL.Query().Get("mode") == "proxy"
	baseURL := getBaseURL(r)
	rewritten := rewriteM3u8(text, targetURL, baseURL, embedOrigin, proxyTs)

	m3u8Response(w, rewritten)
}

// handleTsProxy 处理 /ts/{host}/{path} 的 TS 切片代理
func handleTsProxy(w http.ResponseWriter, r *http.Request) {
	// 解析 /ts/{host}/{path}
	path := strings.TrimPrefix(r.URL.Path, "/ts/")
	slashIdx := strings.Index(path, "/")
	if slashIdx < 0 {
		http.Error(w, "Invalid ts path", 400)
		return
	}

	host := path[:slashIdx]
	remotePath := path[slashIdx:]
	targetURL := fmt.Sprintf("https://%s%s", host, remotePath)

	query := r.URL.Query()
	embedOrigin := normalizeOrigin(query.Get(originQueryParam))
	query.Del(originQueryParam)

	// 保留原始目标 query 参数（排除代理自身参数）
	if remaining := query.Encode(); remaining != "" {
		if strings.Contains(targetURL, "?") {
			targetURL += "&" + remaining
		} else {
			targetURL += "?" + remaining
		}
	}

	logVerbose("代理ts: %s", targetURL)

	client, err := getTLSClient()
	if err != nil {
		http.Error(w, "TLS client error", 500)
		return
	}

	req, err := fhttp.NewRequest(fhttp.MethodGet, targetURL, nil)
	if err != nil {
		http.Error(w, "Bad request", 400)
		return
	}

	if embedOrigin == "" {
		embedOrigin = defaultEmbedOrigin()
	}
	req.Header.Set("Referer", embedOrigin+"/")
	req.Header.Set("Origin", embedOrigin)
	req.Header.Set("Accept", "*/*")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")

	// 透传客户端 Range 请求头（支持断点续传/seek）
	if rangeHeader := r.Header.Get("Range"); rangeHeader != "" {
		req.Header.Set("Range", rangeHeader)
	}

	resp, err := client.Do(req)
	if err != nil {
		logMsg("TS代理失败: %s", err)
		resetTLSClient()
		http.Error(w, "Fetch failed", 502)
		return
	}
	defer resp.Body.Close()

	// 透传必要的响应头
	if ct := resp.Header.Get("Content-Type"); ct != "" {
		w.Header().Set("Content-Type", ct)
	} else {
		w.Header().Set("Content-Type", "video/mp2t")
	}
	if cl := resp.Header.Get("Content-Length"); cl != "" {
		w.Header().Set("Content-Length", cl)
	}
	if cr := resp.Header.Get("Content-Range"); cr != "" {
		w.Header().Set("Content-Range", cr)
	}
	if ar := resp.Header.Get("Accept-Ranges"); ar != "" {
		w.Header().Set("Accept-Ranges", ar)
	}

	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Cache-Control", "no-cache")

	w.WriteHeader(resp.StatusCode)

	// 流式传输，不缓存整个 body
	io.Copy(w, resp.Body)
}

func handleStream(w http.ResponseWriter, r *http.Request, proxyTs bool) {
	cacheKey, embedURL, embedOrigin, err := parseInput(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	mode := "直连CDN"
	if proxyTs {
		mode = "代理ts"
	}
	logVerbose("Stream(%s): %s", mode, cacheKey)
	totalRequests.Add(1)

	m3u8URL, cached, err := extractM3u8(cacheKey, embedURL, embedOrigin)
	if err != nil || m3u8URL == "" {
		errMsg := "M3U8 not found"
		if err != nil {
			errMsg = err.Error()
		}
		http.Error(w, errMsg, 404)
		return
	}

	text, status, err := fetchM3u8Content(m3u8URL, embedOrigin)
	if (err != nil || status != http.StatusOK || !isM3u8Content(text)) && cached {
		// 验证与实际读取之间仍可能刚好过期，再强制重新提取一次。
		cacheMu.Lock()
		delete(m3u8Cache, cacheKey)
		cacheMu.Unlock()
		m3u8URL, _, err = extractM3u8(cacheKey, embedURL, embedOrigin)
		if err == nil && m3u8URL != "" {
			text, status, err = fetchM3u8Content(m3u8URL, embedOrigin)
		}
	}
	if err != nil || status != http.StatusOK || !isM3u8Content(text) {
		http.Error(w, "Failed to fetch m3u8", 502)
		return
	}

	baseURL := getBaseURL(r)
	rewritten := rewriteM3u8(text, m3u8URL, baseURL, embedOrigin, proxyTs)

	m3u8Response(w, rewritten)
}

func handlePlay(w http.ResponseWriter, r *http.Request) {
	cacheKey, embedURL, embedOrigin, err := parseInput(r)
	if err != nil {
		http.Error(w, err.Error(), 400)
		return
	}

	m3u8URL, _, err := extractM3u8(cacheKey, embedURL, embedOrigin)
	if err != nil || m3u8URL == "" {
		http.Error(w, "M3U8 not found", 404)
		return
	}

	// 将 m3u8 URL 转为代理 URL
	baseURL := getBaseURL(r)
	parsed, err := url.Parse(m3u8URL)
	if err != nil {
		http.Error(w, "Invalid m3u8 URL", 500)
		return
	}
	proxyURL := fmt.Sprintf("%s/proxy/%s%s", baseURL, parsed.Host, parsed.RequestURI())
	proxyURL = addProxyParams(proxyURL, embedOrigin, false)

	http.Redirect(w, r, proxyURL, http.StatusFound)
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	cacheMu.RLock()
	cacheSize := len(m3u8Cache)
	cacheMu.RUnlock()

	activePagesMu.Lock()
	ap := activePages
	activePagesMu.Unlock()

	browserMu.Lock()
	browserStatus := "not started"
	if browserCtx != nil {
		browserStatus = "running"
	}
	browserMu.Unlock()

	lastEmbedOriginMu.RLock()
	eo := lastEmbedOrigin
	lastEmbedOriginMu.RUnlock()

	streamIndexMu.RLock()
	streamsPath := streamIndexPath
	streamsCount := len(streamIndex)
	streamIndexMu.RUnlock()

	jsonResponse(w, map[string]interface{}{
		"status":             "running",
		"architecture":       "go (chromedp + tls-client)",
		"port":               config.Port,
		"totalRequests":      totalRequests.Load(),
		"activePages":        ap,
		"maxConcurrent":      config.MaxConcurrent,
		"urlCacheSize":       cacheSize,
		"cacheTTL":           config.CacheTTL.String(),
		"browser":            browserStatus,
		"browserIdleTimeout": config.BrowserIdleTime.String(),
		"embedDomain":        config.EmbedDomain,
		"lastEmbedOrigin":    eo,
		"streamsFile":        streamsPath,
		"streamsCount":       streamsCount,
		"endpoints": map[string]string{
			"/stream?uri=":  "ts直连CDN (uri名称)",
			"/stream?url=":  "ts直连CDN (完整embed URL)",
			"/stream2?uri=": "ts经代理透传 (uri名称)",
			"/stream2?url=": "ts经代理透传 (完整embed URL)",
			"/ts/":          "TS切片代理透传",
		},
	})
}

func handleClearCache(w http.ResponseWriter, r *http.Request) {
	cacheMu.Lock()
	m3u8Cache = make(map[string]*CacheEntry)
	cacheMu.Unlock()
	jsonResponse(w, map[string]interface{}{"success": true})
}

func handleRestart(w http.ResponseWriter, r *http.Request) {
	err := restartBrowser()
	if err != nil {
		jsonResponse(w, map[string]interface{}{"success": false, "error": err.Error()})
		return
	}
	jsonResponse(w, map[string]interface{}{"success": true})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	jsonResponse(w, map[string]interface{}{"status": "ok"})
}

// ============ 主函数 ============

func durationFromEnv(name string, target *time.Duration) {
	raw := os.Getenv(name)
	if raw == "" {
		return
	}
	value, err := time.ParseDuration(raw)
	if err != nil {
		logMsg("⚠ 忽略无效配置 %s=%s: %s", name, raw, err)
		return
	}
	if value <= 0 {
		logMsg("⚠ 忽略非正数配置 %s=%s", name, raw)
		return
	}
	*target = value
}

func main() {
	if p := os.Getenv("PORT"); p != "" {
		if value, err := strconv.Atoi(p); err == nil && value > 0 && value <= 65535 {
			config.Port = value
		}
	}

	if max := os.Getenv("MAX_CONCURRENT"); max != "" {
		if value, err := strconv.Atoi(max); err == nil && value > 0 {
			config.MaxConcurrent = value
		}
	}

	if os.Getenv("VERBOSE") == "1" || os.Getenv("VERBOSE") == "true" {
		config.Verbose = true
	}

	if d := os.Getenv("EMBED_DOMAIN"); d != "" {
		config.EmbedDomain = strings.TrimSuffix(d, "/")
	}

	if streamsFile := os.Getenv("STREAMS_FILE"); streamsFile != "" {
		config.StreamsFile = streamsFile
	}

	durationFromEnv("CACHE_TTL", &config.CacheTTL)
	durationFromEnv("CLEANUP_INTERVAL", &config.CleanupInterval)
	durationFromEnv("BROWSER_IDLE_TIMEOUT", &config.BrowserIdleTime)
	durationFromEnv("EXTRACT_TIMEOUT", &config.ExtractTimeout)
	durationFromEnv("FETCH_TIMEOUT", &config.FetchTimeout)
	updateLastEmbedOrigin(defaultEmbedOrigin())

	if err := reloadStreamIndex(true); err != nil {
		logMsg("⚠ 未载入 streams.json，将使用默认 embed 域名: %s", err)
	}

	fmt.Println(strings.Repeat("=", 50))
	fmt.Println("Modistreams 代理服务 (Go)")
	fmt.Println("  单文件部署: chromedp + tls-client")
	fmt.Println("  支持 uri= 和 url= 两种输入方式")
	fmt.Printf("  默认 embed 域名: %s\n", config.EmbedDomain)
	fmt.Printf("  m3u8 缓存时间: %s（命中时会验证）\n", config.CacheTTL)
	fmt.Printf("  浏览器空闲释放: %s（按需启动）\n", config.BrowserIdleTime)
	fmt.Println("  /stream?uri=   → ts直连CDN")
	fmt.Println("  /stream?url=   → ts直连CDN (完整URL)")
	fmt.Println("  /stream2?uri=  → ts经代理透传")
	fmt.Println("  /stream2?url=  → ts经代理透传 (完整URL)")
	fmt.Println("  /ts/           → TS切片代理透传")
	fmt.Println(strings.Repeat("=", 50))

	go cacheCleanupLoop()
	go browserIdleLoop()

	mux := http.NewServeMux()
	mux.HandleFunc("/m3u8", handleM3u8)
	mux.HandleFunc("/proxy/", handleProxy)
	mux.HandleFunc("/ts/", handleTsProxy) // 新增: TS 切片代理路由
	mux.HandleFunc("/stream", func(w http.ResponseWriter, r *http.Request) {
		handleStream(w, r, false)
	})
	mux.HandleFunc("/stream2", func(w http.ResponseWriter, r *http.Request) {
		handleStream(w, r, true)
	})
	mux.HandleFunc("/play", handlePlay)
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/clear-cache", handleClearCache)
	mux.HandleFunc("/restart", handleRestart)

	srv := &http.Server{
		Addr:    fmt.Sprintf(":%d", config.Port),
		Handler: mux,
	}

	go func() {
		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
		<-sigCh
		logMsg("收到退出信号，正在清理...")
		closeBrowser()
		srv.Shutdown(context.Background())
	}()

	logMsg("✓ 服务就绪: http://0.0.0.0:%d", config.Port)

	if err := srv.ListenAndServe(); err != http.ErrServerClosed {
		log.Fatal(err)
	}
}
