package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func isTLSPort(port int) bool {
	return port == 443 || port == 2053 || port == 2083 || port == 2087 || port == 2096 || port == 8443
}

func runWindowedSpeedTest(ctx context.Context, ip string, port int, customURL string) (float64, string) {
	scheme := "http"
	if isTLSPort(port) {
		scheme = "https"
	}

	parsedURL, err := parseSpeedTestURL(customURL, scheme)
	if err != nil {
		return 0, "URL解析错误: " + err.Error()
	}

	transport := &http.Transport{
		DialContext: func(c context.Context, network, addr string) (net.Conn, error) {
			dialer := &net.Dialer{Timeout: 5 * time.Second}
			return dialer.DialContext(c, "tcp", net.JoinHostPort(ip, strconv.Itoa(port)))
		},
		TLSHandshakeTimeout: 10 * time.Second,
		TLSClientConfig:     tlsConfigWithRootCAs(parsedURL.Hostname()),
		DisableCompression:  true,
	}
	client := http.Client{
		Transport: wrapDebugTransport("official-speed", transport),
		Timeout:   15 * time.Second,
	}

	req, err := http.NewRequestWithContext(ctx, "GET", parsedURL.String(), nil)
	if err != nil {
		return 0, "请求构造错误"
	}
	req.Host = parsedURL.Host
	req.Header.Set("User-Agent", "Mozilla/5.0")
	req.Header.Set("Accept-Encoding", "identity")

	resp, err := client.Do(req)
	if err != nil {
		return 0, "连接错误"
	}
	defer resp.Body.Close()
	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return 0, formatSpeedHTTPFailure(resp.StatusCode)
	}

	buf := make([]byte, 32*1024)
	var totalBytes int64

	timeout := time.After(6 * time.Second)
	measuredStart := time.Now()

	readerCtx, readerCancel := context.WithCancel(ctx)
	defer readerCancel()

	type readChunk struct {
		n   int
		err error
	}
	chunks := make(chan readChunk, 16)
	readerDone := make(chan struct{})
	safeGo("speed-reader", nil, func() {
		defer close(readerDone)
		for {
			n, err := resp.Body.Read(buf)
			select {
			case chunks <- readChunk{n: n, err: err}:
			case <-readerCtx.Done():
				return
			}
			if err != nil {
				return
			}
		}
	})

	done := false
	canceled := false
loop:
	for !done {
		select {
		case <-ctx.Done():
			canceled = true
			done = true
		case <-timeout:
			done = true
		case chunk := <-chunks:
			if chunk.n > 0 {
				totalBytes += int64(chunk.n)
			}
			if chunk.err != nil {
				done = true
				break loop
			}
		}
	}

	readerCancel()
	resp.Body.Close()
	<-readerDone

	if canceled {
		return 0, "测速任务已终止"
	}

	if totalBytes <= 0 {
		return 0, "0MB/s"
	}

	duration := time.Since(measuredStart).Seconds()
	if duration == 0 {
		duration = 1
	}
	return float64(totalBytes) / duration / 1024 / 1024, ""
}

func formatSpeedHTTPFailure(statusCode int) string {
	if statusCode == http.StatusTooManyRequests {
		return "测速失败（速率限制）"
	}
	return "测速失败"
}

func runSpeedTest(ctx context.Context, session *appSession, ip string, port int, customURL string) {
	session.sendWSMessage("log", fmt.Sprintf("开始对 IP %s 端口 %d 进行测速...", ip, port))

	speedMB, speedErr := runWindowedSpeedTest(ctx, ip, port, customURL)
	if speedErr != "" {
		if ctx.Err() != nil {
			return
		}
		session.sendTaskResult(ctx, "speed_test_result", map[string]interface{}{"ip": ip, "speed": speedErr})
		session.sendWSMessage("log", "测速失败: "+speedErr)
		return
	}
	if ctx.Err() != nil {
		return
	}
	speedStr := fmt.Sprintf("%.2fMB/s", speedMB)

	session.sendTaskResult(ctx, "speed_test_result", map[string]interface{}{"ip": ip, "speed": speedStr})
	session.sendWSMessage("log", fmt.Sprintf("IP %s 测速完成: %s", ip, speedStr))
}

const speedRateLimitMessage = "检测到触发速率限制，请稍后重试或更换网络环境"

func isSpeedRateLimited(speedErr string) bool {
	return strings.Contains(speedErr, "速率限制")
}

func runOfficialSpeedTestsCore(ctx context.Context, results []TestResult, port int, limit int, speedMinMB float64, customURL string, onResult func(current, total, qualified int, result TestResult), onRateLimited func()) ([]TestResult, []TestResult) {
	capacity := len(results)
	if limit > 0 && limit < capacity {
		capacity = limit
	}
	qualified := make([]TestResult, 0, capacity)
	interSpeedPause := func() bool {
		select {
		case <-ctx.Done():
			return false
		case <-time.After(1200 * time.Millisecond):
			return true
		}
	}
	consecutiveRateLimited := 0
	for i := range results {
		select {
		case <-ctx.Done():
			return results, qualified
		default:
		}
		if limit > 0 && len(qualified) >= limit {
			break
		}
		speedMB, speedErr := runWindowedSpeedTest(ctx, results[i].IP, port, customURL)
		if speedErr != "" {
			results[i].Speed = speedErr
			if isSpeedRateLimited(speedErr) {
				consecutiveRateLimited++
			} else {
				consecutiveRateLimited = 0
			}
		} else {
			consecutiveRateLimited = 0
			results[i].Speed = fmt.Sprintf("%.2fMB/s", speedMB)
			if speedMB >= speedMinMB {
				qualified = append(qualified, results[i])
			}
		}
		if onResult != nil {
			if ctx.Err() != nil {
				return results, qualified
			}
			onResult(i+1, len(results), len(qualified), results[i])
		}
		if !interSpeedPause() {
			return results, qualified
		}
		if consecutiveRateLimited >= 3 {
			if onRateLimited != nil {
				onRateLimited()
			}
			break
		}
	}
	return results, qualified
}

func runOfficialSpeedBatch(ctx context.Context, session *appSession, port int, customURL string, speedLimit int, speedMin float64, fallbackResults []TestResult, skipTested bool) {
	if port <= 0 {
		port = 443
	}
	if speedLimit <= 0 {
		session.sendWSMessage("log", "官方批量测速已关闭（测速上限 0）")
		session.sendWSMessage("official_speed_complete", map[string]interface{}{"qualified": 0, "limit": speedLimit, "rateLimited": false})
		return
	}
	if speedMin <= 0 {
		speedMin = 0.1
	}

	session.testMutex.Lock()
	results := append([]TestResult(nil), session.testResults...)
	session.testMutex.Unlock()
	if len(results) == 0 && len(fallbackResults) > 0 {
		results = append([]TestResult(nil), fallbackResults...)
		session.testMutex.Lock()
		session.testResults = append([]TestResult(nil), results...)
		session.testMutex.Unlock()
	}
	if len(results) == 0 {
		session.sendWSMessage("log", "没有可用的详细测试结果，跳过官方批量测速")
		session.sendWSMessage("official_speed_complete", map[string]interface{}{"qualified": 0, "limit": speedLimit, "rateLimited": false})
		return
	}
	if skipTested {
		pending := results[:0]
		for _, result := range results {
			speedText := strings.TrimSpace(result.Speed)
			if speedText == "" || speedText == "未测速" {
				pending = append(pending, result)
			}
		}
		results = pending
		if len(results) == 0 {
			session.sendWSMessage("log", "没有未测速的详细测试结果，跳过继续测速")
			session.sendWSMessage("official_speed_complete", map[string]interface{}{"qualified": 0, "limit": speedLimit, "rateLimited": false})
			return
		}
	}
	sortOfficialTestResults(results)

	session.sendWSMessage("log", fmt.Sprintf("开始测速：%d 条记录，目标上限=%d，测速阈值=%.2fMB/s", len(results), speedLimit, speedMin))
	session.sendWSMessage("official_speed_progress", map[string]interface{}{"current": 0, "total": len(results), "qualified": 0, "limit": speedLimit})
	rateLimited := false
	updated, qualified := runOfficialSpeedTestsCore(ctx, results, port, speedLimit, speedMin, customURL, func(current, total, qualified int, result TestResult) {
		session.sendTaskResult(ctx, "speed_test_result", map[string]interface{}{"ip": result.IP, "speed": result.Speed})
		session.sendWSMessage("official_speed_progress", map[string]interface{}{"current": current, "total": total, "qualified": qualified, "limit": speedLimit})
	}, func() {
		rateLimited = true
		session.sendWSMessage("log", speedRateLimitMessage)
	})

	session.testMutex.Lock()
	byIP := make(map[string]string, len(updated))
	for _, result := range updated {
		byIP[result.IP] = result.Speed
	}
	for idx := range session.testResults {
		if speed, ok := byIP[session.testResults[idx].IP]; ok {
			session.testResults[idx].Speed = speed
		}
	}
	totalQualified := countOfficialQualifiedResults(session.testResults, speedMin)
	session.testMutex.Unlock()
	if ctx.Err() != nil {
		session.sendWSMessage("log", "官方批量测速已终止")
	} else if rateLimited {
		session.sendWSMessage("log", fmt.Sprintf("官方批量测速已因速率限制停止，达标 %d/%d，总达标 %d", len(qualified), speedLimit, totalQualified))
	} else {
		session.sendWSMessage("log", fmt.Sprintf("官方批量测速完成，达标 %d/%d，总达标 %d", len(qualified), speedLimit, totalQualified))
	}
	if ctx.Err() == nil {
		session.sendWSMessage("official_speed_complete", map[string]interface{}{"qualified": len(qualified), "totalQualified": totalQualified, "limit": speedLimit, "rateLimited": rateLimited})
	}
}

func countOfficialQualifiedResults(results []TestResult, speedMin float64) int {
	count := 0
	for _, result := range results {
		if speed, ok := parseSpeedMBForSort(result.Speed); ok && speed >= speedMin {
			count++
		}
	}
	return count
}
