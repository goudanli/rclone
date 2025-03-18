package accounting

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/rclone/rclone/fs"
)

// TaskReport 表示要发送到服务器的任务完成状态
type TaskReport struct {
	Timestamp        time.Time `json:"timestamp"`
	BytesTransferred int64     `json:"bytes_transferred"`
	TotalFiles       int64     `json:"total_files"`
	Duration         float64   `json:"duration_seconds"`
	Success          bool      `json:"success"`
	ErrorCount       int64     `json:"error_count"`
	LastError        string    `json:"last_error,omitempty"`
	RecordID         string    `json:"record_id"`
	BusinessType     string    `json:"business_type"`
}

// TaskReporter 负责向服务器报告任务完成状态
type TaskReporter struct {
	ctx                context.Context
	stats              *StatsInfo
	serverURL          string
	httpClient         *http.Client
	recordID           string
	businessType       string
	timepoint          string
	updateBackupStatus bool
	mu                 sync.RWMutex
	reportSuccess      bool
}

// NewTaskReporter 创建一个新的任务报告器
func NewTaskReporter(ctx context.Context, stats *StatsInfo, serverURL string) *TaskReporter {
	// 创建自定义Transport
	tr := &http.Transport{
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true, // 关闭证书验证
		},
	}
	return &TaskReporter{
		ctx:                ctx,
		stats:              stats,
		serverURL:          serverURL,
		httpClient:         &http.Client{Timeout: 30 * time.Second, Transport: tr},
		recordID:           stats.ci.BackupRecordID,
		businessType:       stats.ci.BackupBusinessType,
		timepoint:          stats.ci.BackupTimepoint,
		updateBackupStatus: stats.ci.UpdateBackupStatus,
	}
}

// ReportTaskComplete 发送任务完成状态到服务器
func (tr *TaskReporter) ReportTaskComplete(status int) error {
	tr.mu.Lock()
	defer tr.mu.Unlock()
	if tr.reportSuccess {
		return nil
	}
	if status != StatusStop && !tr.updateBackupStatus {
		return nil
	}
	// report := tr.generateReport()

	// data, err := json.Marshal(report)
	// if err != nil {
	// 	return fmt.Errorf("error marshaling report: %w", err)
	// }
	// fmt.Println("report:", string(data))
	// 先打印日志，方便调试
	// fs.Infof(nil, "Sending task completion report: %s", string(data))

	req, err := http.NewRequest("POST", tr.serverURL+"/updateRecordStatus", nil)
	if err != nil {
		return fmt.Errorf("error creating request: %w", err)
	}
	values := req.URL.Query()
	values.Add("recordId", tr.recordID)
	values.Add("businessType", tr.businessType)
	values.Add("status", strconv.Itoa(status))
	if tr.timepoint != "" {
		values.Add("timepoint", tr.timepoint)
	}
	req.URL.RawQuery = values.Encode()
	fs.Logf(nil, req.URL.String())
	req.Header.Set("Content-Type", "application/json")

	resp, err := tr.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("error sending request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	fs.Logf(nil, "Task completion report sent successfully")
	tr.reportSuccess = true
	return nil
}

// generateReport 生成任务完成报告
func (tr *TaskReporter) generateReport() TaskReport {
	tr.stats.mu.RLock()
	defer tr.stats.mu.RUnlock()

	var lastErrorStr string
	if tr.stats.lastError != nil {
		lastErrorStr = tr.stats.lastError.Error()
	}

	return TaskReport{
		Timestamp:        time.Now(),
		BytesTransferred: tr.stats.bytes,
		TotalFiles:       tr.stats.transfers,
		Duration:         time.Since(tr.stats.startTime).Seconds(),
		Success:          tr.stats.errors == 0,
		ErrorCount:       tr.stats.errors,
		LastError:        lastErrorStr,
		RecordID:         tr.recordID,
		BusinessType:     tr.businessType,
	}
}
