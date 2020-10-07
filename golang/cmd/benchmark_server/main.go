package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	_ "net/http/pprof"
	"time"

	"github.com/jmoiron/sqlx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	xsuportal "github.com/isucon/isucon10-final/webapp/golang"
	"github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
	resourcespb "github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
	"github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/services/bench"
	"github.com/isucon/isucon10-final/webapp/golang/util"
)

var db *sqlx.DB

const RedisHostPrivateIPAddress = "10.162.10.102"
const LeaderBoardServerKey = "board"

var isMasterServerIP = MyServerIsOnMasterServerIP()
var idToLeaderBoardServer = NewSyncMapServerConn(GetMasterServerAddress()+":8884", isMasterServerIP)

type benchmarkQueueService struct {
}

func (b *benchmarkQueueService) Svc() *bench.BenchmarkQueueService {
	return &bench.BenchmarkQueueService{
		ReceiveBenchmarkJob: b.ReceiveBenchmarkJob,
	}
}

func (b *benchmarkQueueService) ReceiveBenchmarkJob(ctx context.Context, req *bench.ReceiveBenchmarkJobRequest) (*bench.ReceiveBenchmarkJobResponse, error) {
	jobResponse := &bench.ReceiveBenchmarkJobResponse{}
	var job xsuportal.BenchmarkJob

	randomBytes := make([]byte, 16)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return jobResponse, fmt.Errorf("read random: %w", err)
	}
	handle := base64.StdEncoding.EncodeToString(randomBytes)
	jobID := <-jobQue
	_, err = db.Exec(
		"UPDATE `benchmark_jobs` SET `status` = ?, `handle` = ? WHERE `id` = ? AND `status` = ? LIMIT 1",
		resources.BenchmarkJob_SENT,
		handle,
		jobID,
		resources.BenchmarkJob_PENDING,
	)
	if err != nil {
		return jobResponse, fmt.Errorf("update benchmark job status: %w", err)
	}

	var contestStartsAt time.Time
	err = db.Get(&contestStartsAt, "SELECT `contest_starts_at` FROM `contest_config` LIMIT 1")
	if err != nil {
		return jobResponse, fmt.Errorf("get contest starts at: %w", err)
	}

	err = db.Get(&job, "SELECT * from `benchmark_jobs` where id = ?",
		jobID,
	)
	if err != nil {
		return jobResponse, fmt.Errorf("get benchmark job: %w", err)
	}

	jobResponse.JobHandle = &bench.ReceiveBenchmarkJobResponse_JobHandle{
		JobId:            job.ID,
		Handle:           handle,
		TargetHostname:   job.TargetHostName,
		ContestStartedAt: timestamppb.New(contestStartsAt),
		JobCreatedAt:     timestamppb.New(job.CreatedAt),
	}
	if jobResponse.JobHandle != nil {
		log.Printf("[DEBUG] Dequeued: job_handle=%+v", jobResponse.JobHandle)
	}
	return jobResponse, nil
}

type benchmarkReportService struct {
}

func (b *benchmarkReportService) Svc() *bench.BenchmarkReportService {
	return &bench.BenchmarkReportService{
		ReportBenchmarkResult: b.ReportBenchmarkResult,
	}
}

func (b *benchmarkReportService) ReportBenchmarkResult(srv bench.BenchmarkReport_ReportBenchmarkResultServer) error {
	var notifier xsuportal.Notifier
	for {
		req, err := srv.Recv()
		if err != nil {
			return err
		}
		if req.Result == nil {
			return status.Error(codes.InvalidArgument, "result required")
		}

		err = func() error {
			tx, err := db.Beginx()
			if err != nil {
				return fmt.Errorf("begin tx: %w", err)
			}
			defer tx.Rollback()

			var job xsuportal.BenchmarkJob
			err = tx.Get(
				&job,
				"SELECT * FROM `benchmark_jobs` WHERE `id` = ? AND `handle` = ? LIMIT 1 FOR UPDATE",
				req.JobId,
				req.Handle,
			)
			if err == sql.ErrNoRows {
				log.Printf("[ERROR] Job not found: job_id=%v, handle=%+v", req.JobId, req.Handle)
				return status.Errorf(codes.NotFound, "Job %d not found or handle is wrong", req.JobId)
			}
			if err != nil {
				return fmt.Errorf("get benchmark job: %w", err)
			}
			if req.Result.Finished {
				log.Printf("[DEBUG] %v: save as finished", req.JobId)
				if err := b.saveAsFinished(tx, &job, req); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit tx: %w", err)
				}
				if err := notifier.NotifyBenchmarkJobFinished(db, &job); err != nil {
					return fmt.Errorf("notify benchmark job finished: %w", err)
				}
			} else {
				log.Printf("[DEBUG] %v: save as running", req.JobId)
				if err := b.saveAsRunning(tx, &job, req); err != nil {
					return err
				}
				if err := tx.Commit(); err != nil {
					return fmt.Errorf("commit tx: %w", err)
				}
			}
			return nil
		}()
		if err != nil {
			return err
		}
		err = srv.Send(&bench.ReportBenchmarkResultResponse{
			AckedNonce: req.GetNonce(),
		})
		if err != nil {
			return fmt.Errorf("send report: %w", err)
		}
	}
}

func toTimestamp(t sql.NullTime) *timestamppb.Timestamp {
	if t.Valid {
		return timestamppb.New(t.Time)
	}
	return nil
}

func (b *benchmarkReportService) saveAsFinished(db sqlx.Execer, job *xsuportal.BenchmarkJob, req *bench.ReportBenchmarkResultRequest) error {
	if !job.StartedAt.Valid || job.FinishedAt.Valid {
		return status.Errorf(codes.FailedPrecondition, "Job %v has already finished or has not started yet", req.JobId)
	}
	if req.Result.MarkedAt == nil {
		return status.Errorf(codes.InvalidArgument, "marked_at is required")
	}
	markedAt := req.Result.MarkedAt.AsTime().Round(time.Microsecond)

	result := req.Result
	var raw, deduction sql.NullInt32
	if result.ScoreBreakdown != nil {
		raw.Valid = true
		raw.Int32 = int32(result.ScoreBreakdown.Raw)
		deduction.Valid = true
		deduction.Int32 = int32(result.ScoreBreakdown.Deduction)
	}
	_, err := db.Exec(
		"UPDATE `benchmark_jobs` SET `status` = ?, `score_raw` = ?, `score_deduction` = ?, `passed` = ?, `reason` = ?, `updated_at` = NOW(6), `finished_at` = ? WHERE `id` = ? LIMIT 1",
		resources.BenchmarkJob_FINISHED,
		raw,
		deduction,
		result.Passed,
		result.Reason,
		markedAt,
		req.JobId,
	)
	if err != nil {
		return fmt.Errorf("update benchmark job status: %w", err)
	}

	// update leaderboard on memory
	var leaderboard *resources.Leaderboard = &resources.Leaderboard{}
	fmt.Println(idToLeaderBoardServer.AllKeys())
	if idToLeaderBoardServer.Exists(LeaderBoardServerKey) {

		ok := idToLeaderBoardServer.Get(LeaderBoardServerKey, leaderboard)
		if !ok {
			fmt.Println("not ok")
			return nil
		}
		score := int64(raw.Int32 - deduction.Int32)
		for _, v := range leaderboard.Teams {
			if v.Team.Id == job.TeamID {
				v.Scores = append(v.Scores, &resources.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				})
				if v.BestScore.Score <= score {
					v.BestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
						Score:     score,
						StartedAt: toTimestamp(job.StartedAt),
						MarkedAt:  result.MarkedAt,
					}
				}
				v.LatestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				}
				v.FinishCount = v.FinishCount + 1
				break
			}
		}
		for _, v := range leaderboard.GeneralTeams {
			if v.Team.Id == job.TeamID {
				v.Scores = append(v.Scores, &resources.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				})
				if v.BestScore.Score <= score {
					v.BestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
						Score:     score,
						StartedAt: toTimestamp(job.StartedAt),
						MarkedAt:  result.MarkedAt,
					}
				}
				v.LatestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				}
				v.FinishCount = v.FinishCount + 1
				break
			}
		}
		for _, v := range leaderboard.StudentTeams {
			if v.Team.Id == job.TeamID {
				v.Scores = append(v.Scores, &resources.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				})
				if v.BestScore.Score <= score {
					v.BestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
						Score:     score,
						StartedAt: toTimestamp(job.StartedAt),
						MarkedAt:  result.MarkedAt,
					}
				}
				v.LatestScore = &resourcespb.Leaderboard_LeaderboardItem_LeaderboardScore{
					Score:     score,
					StartedAt: toTimestamp(job.StartedAt),
					MarkedAt:  result.MarkedAt,
				}
				v.FinishCount = v.FinishCount + 1
				break
			}
		}
		fmt.Println("update leaderboard on memory")
		idToLeaderBoardServer.FlushAll()
		idToLeaderBoardServer.Set(LeaderBoardServerKey, *leaderboard)

	}
	return nil
}

func (b *benchmarkReportService) saveAsRunning(db sqlx.Execer, job *xsuportal.BenchmarkJob, req *bench.ReportBenchmarkResultRequest) error {
	if req.Result.MarkedAt == nil {
		return status.Errorf(codes.InvalidArgument, "marked_at is required")
	}
	var startedAt time.Time
	if job.StartedAt.Valid {
		startedAt = job.StartedAt.Time
	} else {
		startedAt = req.Result.MarkedAt.AsTime().Round(time.Microsecond)
	}
	_, err := db.Exec(
		"UPDATE `benchmark_jobs` SET `status` = ?, `score_raw` = NULL, `score_deduction` = NULL, `passed` = FALSE, `reason` = NULL, `started_at` = ?, `updated_at` = NOW(6), `finished_at` = NULL WHERE `id` = ? LIMIT 1",
		resources.BenchmarkJob_RUNNING,
		startedAt,
		req.JobId,
	)
	if err != nil {
		return fmt.Errorf("update benchmark job status: %w", err)
	}
	return nil
}

func pollBenchmarkJob(db sqlx.Queryer) (*xsuportal.BenchmarkJob, error) {
	for i := 0; i < 10; i++ {
		if i >= 1 {
			time.Sleep(50 * time.Millisecond)
		}
		var job xsuportal.BenchmarkJob
		err := sqlx.Get(
			db,
			&job,
			"SELECT * FROM `benchmark_jobs` WHERE `status` = ? ORDER BY `id` LIMIT 1",
			resources.BenchmarkJob_PENDING,
		)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("get benchmark job: %w", err)
		}
		return &job, nil
	}
	return nil, nil
}

var jobQue chan int64

func main() {
	go func() { log.Println(http.ListenAndServe(":9009", nil)) }()
	// benchmark job queue
	go func() {
		serverMux := http.NewServeMux()
		serverMux.HandleFunc("/enque", func(w http.ResponseWriter, req *http.Request) {
			defer req.Body.Close()
			body, err := ioutil.ReadAll(req.Body)
			if err != nil {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			jobID := int64(binary.BigEndian.Uint64(body))
			jobQue <- jobID
		})
		log.Println(http.ListenAndServe(":9999", serverMux))
	}()
	jobQue = make(chan int64, 1000)
	port := util.GetEnv("PORT", "50051")
	address := ":" + port

	listener, err := net.Listen("tcp", address)
	if err != nil {
		panic(err)
	}
	log.Print("[INFO] listen ", address)

	db, _ = xsuportal.GetDB()
	db.SetMaxOpenConns(40)

	server := grpc.NewServer()

	queue := &benchmarkQueueService{}
	report := &benchmarkReportService{}

	bench.RegisterBenchmarkQueueService(server, queue.Svc())
	bench.RegisterBenchmarkReportService(server, report.Svc())

	if err := server.Serve(listener); err != nil {
		panic(err)
	}
}
