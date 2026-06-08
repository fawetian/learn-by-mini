package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/hibiken/asynq"
)

const (
	taskTypeWriteAudit  = "audit:write"
	taskTypeDailyReport = "report:daily"

	defaultRedisAddr = "127.0.0.1:6379"
	outputDir        = "runtime"
)

type auditPayload struct {
	EventID   string    `json:"event_id"`
	UserID    int       `json:"user_id"`
	Action    string    `json:"action"`
	CreatedAt time.Time `json:"created_at"`
}

type dailyReportPayload struct {
	Date string `json:"date"`
}

func main() {
	log.SetFlags(log.LstdFlags | log.Lmicroseconds)

	if len(os.Args) < 2 {
		printUsage()
		os.Exit(2)
	}

	redisOpt := asynq.RedisClientOpt{
		Addr:     getenv("REDIS_ADDR", defaultRedisAddr),
		Password: os.Getenv("REDIS_PASSWORD"),
		DB:       getenvInt("REDIS_DB", 0),
	}

	var err error
	switch os.Args[1] {
	case "worker":
		err = runWorker(redisOpt, os.Args[2:])
	case "enqueue":
		err = runEnqueue(redisOpt, os.Args[2:])
	default:
		printUsage()
		os.Exit(2)
	}
	if err != nil {
		log.Fatal(err)
	}
}

func runWorker(redisOpt asynq.RedisClientOpt, args []string) error {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	concurrency := fs.Int("concurrency", 4, "worker 并发数")
	if err := fs.Parse(args); err != nil {
		return err
	}

	srv := asynq.NewServer(redisOpt, asynq.Config{
		Concurrency: *concurrency,
		Queues: map[string]int{
			"critical": 6,
			"default":  3,
			"low":      1,
		},
	})

	mux := asynq.NewServeMux()
	mux.HandleFunc(taskTypeWriteAudit, handleWriteAudit)
	mux.HandleFunc(taskTypeDailyReport, handleDailyReport)

	log.Printf("worker 已启动: redis=%s db=%d concurrency=%d", redisOpt.Addr, redisOpt.DB, *concurrency)
	return srv.Run(mux)
}

func runEnqueue(redisOpt asynq.RedisClientOpt, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	userID := fs.Int("user", 1001, "用户编号")
	action := fs.String("action", "signup", "用户动作")
	queue := fs.String("queue", "default", "任务队列")
	if err := fs.Parse(args); err != nil {
		return err
	}

	client := asynq.NewClient(redisOpt)
	defer client.Close()

	auditTask, err := newAuditTask(*userID, *action)
	if err != nil {
		return err
	}
	auditInfo, err := client.Enqueue(
		auditTask,
		asynq.Queue(*queue),
		asynq.MaxRetry(3),
		asynq.Timeout(30*time.Second),
		asynq.Retention(24*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("提交审计任务: %w", err)
	}
	log.Printf("审计任务已入队: id=%s queue=%s", auditInfo.ID, auditInfo.Queue)

	reportTask, err := newDailyReportTask(time.Now())
	if err != nil {
		return err
	}
	reportInfo, err := client.Enqueue(
		reportTask,
		asynq.Queue("low"),
		asynq.ProcessIn(10*time.Second),
		asynq.MaxRetry(2),
		asynq.Timeout(30*time.Second),
		asynq.Retention(24*time.Hour),
	)
	if err != nil {
		return fmt.Errorf("提交日报任务: %w", err)
	}
	log.Printf("日报任务已入队: id=%s queue=%s 约 10 秒后执行", reportInfo.ID, reportInfo.Queue)

	return nil
}

func newAuditTask(userID int, action string) (*asynq.Task, error) {
	payload, err := json.Marshal(auditPayload{
		EventID:   fmt.Sprintf("evt_%d_%d", userID, time.Now().UnixNano()),
		UserID:    userID,
		Action:    action,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return nil, fmt.Errorf("编码审计任务: %w", err)
	}
	return asynq.NewTask(taskTypeWriteAudit, payload), nil
}

func newDailyReportTask(now time.Time) (*asynq.Task, error) {
	payload, err := json.Marshal(dailyReportPayload{
		Date: now.Format("2006-01-02"),
	})
	if err != nil {
		return nil, fmt.Errorf("编码日报任务: %w", err)
	}
	return asynq.NewTask(taskTypeDailyReport, payload), nil
}

func handleWriteAudit(ctx context.Context, task *asynq.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var payload auditPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("解析审计任务载荷失败: %v: %w", err, asynq.SkipRetry)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("创建输出目录: %w", err)
	}

	line, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("编码审计日志: %w", err)
	}

	path := filepath.Join(outputDir, "audit.log")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return fmt.Errorf("打开审计日志: %w", err)
	}
	defer file.Close()

	if _, err := file.Write(append(line, '\n')); err != nil {
		return fmt.Errorf("写入审计日志: %w", err)
	}

	log.Printf("审计任务完成: event_id=%s user_id=%d action=%s path=%s", payload.EventID, payload.UserID, payload.Action, path)
	return nil
}

func handleDailyReport(ctx context.Context, task *asynq.Task) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	var payload dailyReportPayload
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		return fmt.Errorf("解析日报任务载荷失败: %v: %w", err, asynq.SkipRetry)
	}

	if err := os.MkdirAll(outputDir, 0o755); err != nil {
		return fmt.Errorf("创建输出目录: %w", err)
	}

	path := filepath.Join(outputDir, "report-"+payload.Date+".txt")
	content := fmt.Sprintf("date=%s generated_at=%s\n", payload.Date, time.Now().Format(time.RFC3339))
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		return fmt.Errorf("写入日报文件: %w", err)
	}

	log.Printf("日报任务完成: date=%s path=%s", payload.Date, path)
	return nil
}

func getenv(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}

func getenvInt(key string, fallback int) int {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	n, err := strconv.Atoi(value)
	if err != nil {
		log.Printf("忽略无效的 %s=%q，使用默认值 %d", key, value, fallback)
		return fallback
	}
	return n
}

func printUsage() {
	fmt.Println(`用法:
  go run . worker [-concurrency 4]
  go run . enqueue [-user 1001] [-action signup] [-queue default]

环境变量:
  REDIS_ADDR      Redis 地址，默认 127.0.0.1:6379
  REDIS_PASSWORD  Redis 密码，默认空
  REDIS_DB        Redis 数据库编号，默认 0`)
}
