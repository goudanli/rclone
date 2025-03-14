package accounting

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
)

// BackupReporter 负责定期向服务器报告备份数据量
type BackupReporter struct {
	ctx          context.Context
	cancel       context.CancelFunc
	stats        *StatsInfo
	serverURL    string
	interval     time.Duration
	httpClient   *http.Client
	wg           sync.WaitGroup
	recordID     string
	businessType string
}

// BackupReport 表示要发送到服务器的数据结构
type BackupReport struct {
	Timestamp        time.Time `json:"timestamp"`
	BytesTransferred int64     `json:"bytes_transferred"`
	TotalFiles       int64     `json:"total_files"`
	Speed            float64   `json:"speed"`
	Errors           int64     `json:"errors"`
	RecordID         string    `json:"record_id"`
	BusinessType     string    `json:"business_type"`
}

// NewBackupReporter 创建一个新的备份报告器
func NewBackupReporter(ctx context.Context, stats *StatsInfo, serverURL string) *BackupReporter {
	ctx, cancel := context.WithCancel(ctx)
	// 创建自定义Transport
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // 关闭证书验证
		},
	}
	return &BackupReporter{
		ctx:          ctx,
		cancel:       cancel,
		stats:        stats,
		serverURL:    serverURL,
		interval:     2 * time.Second,
		httpClient:   &http.Client{Timeout: 20 * time.Second, Transport: tr},
		recordID:     stats.ci.BackupRecordID,
		businessType: stats.ci.BackupBusinessType,
	}
}

// Start 开始定期报告
func (br *BackupReporter) Start() {
	br.wg.Add(1)
	go br.reportLoop()
	fs.Logf(nil, "processId:"+strconv.Itoa(os.Getpid()))
}

// Stop 停止报告
func (br *BackupReporter) Stop() {
	br.cancel()
	br.wg.Wait()
}

// reportLoop 是主要的报告循环
func (br *BackupReporter) reportLoop() {
	defer br.wg.Done()
	ticker := time.NewTicker(br.interval)
	defer ticker.Stop()

	for {
		select {
		case <-br.ctx.Done():
			if err := br.SendReport(); err != nil {
				fs.Errorf(nil, "Failed to send backup report: %v", err)
			}
			return
		case <-ticker.C:
			if err := br.SendReport(); err != nil {
				fs.Errorf(nil, "Failed to send backup report: %v", err)
			}
			if err := br.sendHeartBeat(); err != nil {
				fs.Errorf(nil, "Failed to send heart beat: %v", err)
			}
		}
	}
}

// sendReport 发送单个报告到服务器
func (br *BackupReporter) SendReport() error {
	// report := br.generateReport()
	// data, err := json.Marshal(report)
	// if err != nil {
	// 	return fmt.Errorf("error marshaling report: %w", err)
	// }
	// fmt.Println("report:", string(data))

	ts := br.stats.calculateTransferStats()

	req, err := http.NewRequest("POST", br.serverURL+"/updateStatistics", nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	values := req.URL.Query()
	values.Add("recordId", br.recordID)
	values.Add("businessType", br.businessType)
	values.Add("total", strconv.FormatInt(ts.totalBytes+br.stats.statsBaseSize, 10))
	values.Add("backuped", strconv.FormatInt(br.stats.bytes+br.stats.statsBaseSize, 10))
	values.Add("transferred", strconv.FormatInt(br.stats.bytes+br.stats.statsBaseSize, 10))
	values.Add("speed", strconv.FormatInt(int64(ts.speed), 10))
	req.URL.RawQuery = values.Encode()
	fs.Infof(nil, req.URL.String())
	req.Header.Set("Content-Type", "application/json")

	resp, err := br.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return nil
}

func (br *BackupReporter) sendHeartBeat() error {
	req, err := http.NewRequestWithContext(br.ctx, "POST", br.serverURL+"/updateHeartBeatTimer", nil)
	values := req.URL.Query()
	values.Add("recordId", br.recordID)
	values.Add("businessType", br.businessType)
	values.Add("processId", strconv.Itoa(os.Getpid()))
	req.URL.RawQuery = values.Encode()
	fs.Infof(nil, req.URL.String())
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")

	resp, err := br.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}
	return nil
}

// generateReport 生成当前的备份报告
func (br *BackupReporter) generateReport() BackupReport {
	br.stats.mu.RLock()
	defer br.stats.mu.RUnlock()

	return BackupReport{
		Timestamp:        time.Now(),
		BytesTransferred: br.stats.bytes,
		TotalFiles:       br.stats.transfers,
		Speed:            br.stats._speed(),
		Errors:           br.stats.errors,
		RecordID:         br.recordID,
		BusinessType:     br.businessType,
	}
}
