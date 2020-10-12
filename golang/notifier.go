package xsuportal

import (
	"crypto/elliptic"
	"crypto/x509"
	"database/sql"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/SherClockHolmes/webpush-go"
	"github.com/golang/protobuf/proto"
	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/singleflight"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/isucon/isucon10-final/webapp/golang/proto/xsuportal/resources"
)

const (
	WebpushVAPIDPrivateKeyPath = "../vapid_private.pem"
	WebpushSubject             = "xsuportal@example.com"
)

var client = &http.Client{}

func init() {
	transport := http.DefaultTransport.(*http.Transport)
	transport.MaxIdleConnsPerHost = 500
	client.Transport = transport
}

type Notifier struct {
	mu      sync.Mutex
	options *webpush.Options
}

func (n *Notifier) VAPIDKey() *webpush.Options {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.options == nil {
		pemBytes, err := ioutil.ReadFile(WebpushVAPIDPrivateKeyPath)
		if err != nil {
			return nil
		}
		block, _ := pem.Decode(pemBytes)
		if block == nil {
			return nil
		}
		priKey, err := x509.ParseECPrivateKey(block.Bytes)
		if err != nil {
			return nil
		}
		priBytes := priKey.D.Bytes()
		pubBytes := elliptic.Marshal(priKey.Curve, priKey.X, priKey.Y)
		pri := base64.RawURLEncoding.EncodeToString(priBytes)
		pub := base64.RawURLEncoding.EncodeToString(pubBytes)
		n.options = &webpush.Options{
			Subscriber:      WebpushSubject,
			VAPIDPrivateKey: pri,
			VAPIDPublicKey:  pub,
		}
	}
	return n.options
}

func (n *Notifier) NotifyClarificationAnswered(db sqlx.Ext, c *Clarification, updated bool) error {
	var contestants []struct {
		ID     string `db:"id"`
		TeamID int64  `db:"team_id"`
	}
	if c.Disclosed.Valid && c.Disclosed.Bool {
		keys := ContestantServer.AllKeys()
		teamIDs := make([]string, 0, 100)
		for _, v := range keys {
			// contestantID is string, teamID is int
			_, err := strconv.ParseInt(v, 10, 64)
			if err == nil { // if key is teamID
				teamIDs = append(teamIDs, v)
			}
		}
		result := ContestantServer.MGet(teamIDs)
		for _, v := range teamIDs {
			memberIDs := make([]string, 0, 3)
			result.Get(v, &memberIDs)
			for _, id := range memberIDs {
				teamID, _ := strconv.ParseInt(v, 10, 64)
				contestants = append(contestants,
					struct {
						ID     string `db:"id"`
						TeamID int64  `db:"team_id"`
					}{ID: id, TeamID: teamID})

			}
		}
		// err := sqlx.Select(
		// 	db,
		// 	&contestants,
		// 	"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` IS NOT NULL",
		// )
		// if err != nil {
		// 	return fmt.Errorf("select all contestants: %w", err)
		// }
	} else {
		memberIDs := make([]string, 0, 3)
		ContestantServer.Get(strconv.FormatInt(c.TeamID, 10), &memberIDs)
		for _, v := range memberIDs {
			contestants = append(contestants,
				struct {
					ID     string `db:"id"`
					TeamID int64  `db:"team_id"`
				}{ID: v, TeamID: c.TeamID})
		}
		// err := sqlx.Select(
		// 	db,
		// 	&contestants,
		// 	"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` = ?",
		// 	c.TeamID,
		// )
		// if err != nil {
		// 	return fmt.Errorf("select contestants(team_id=%v): %w", c.TeamID, err)
		// }
	}
	notificationPBs := make(map[string]*resources.Notification)
	for _, contestant := range contestants {
		notificationPB := &resources.Notification{
			Content: &resources.Notification_ContentClarification{
				ContentClarification: &resources.Notification_ClarificationMessage{
					ClarificationId: c.ID,
					Owned:           c.TeamID == contestant.TeamID,
					Updated:         updated,
				},
			},
		}
		notificationPBs[contestant.ID] = notificationPB
	}

	go func() {
		_, err := n.bulkNotify(db, notificationPBs)
		if err != nil {
			log.Println("notify: ", err)
		}
	}()
	return nil
}

func (n *Notifier) bulkNotify(db sqlx.Ext, notificationPBs map[string]*resources.Notification) (*Notification, error) {
	now := time.Now().Round(time.Microsecond)
	lastInsertID := rand.Int63()
	for i, v := range notificationPBs {
		v.Id = lastInsertID
		v.CreatedAt = timestamppb.New(now)

		subscriptions, err := getPushSubscriptions(db, i)
		if err != nil {
			return nil, fmt.Errorf("get push subscriptions: %w", err)
		}

		for _, subscription := range subscriptions {
			go func(rn *resources.Notification, s PushSubscription) {
				err = n.SendWebPush(rn, &s)
				if err != nil {
					log.Println("send webpush: ", err)
				}
			}(v, subscription)
		}
	}
	return nil, nil
}

func (n *Notifier) NotifyBenchmarkJobFinished(db sqlx.Ext, job *BenchmarkJob) error {
	var contestants []struct {
		ID     string `db:"id"`
		TeamID int64  `db:"team_id"`
	}
	memberIDs := make([]string, 0, 3)
	ContestantServer.Get(strconv.FormatInt(job.TeamID, 10), &memberIDs)
	for _, v := range memberIDs {
		contestants = append(contestants,
			struct {
				ID     string `db:"id"`
				TeamID int64  `db:"team_id"`
			}{ID: v, TeamID: job.TeamID})
	}
	// err := sqlx.Select(
	// 	db,
	// 	&contestants,
	// 	"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` = ?",
	// 	job.TeamID,
	// )
	// if err != nil {
	// 	return fmt.Errorf("select contestants(team_id=%v): %w", job.TeamID, err)
	// }
	notificationPBs := make(map[string]*resources.Notification)
	for _, contestant := range contestants {
		notificationPB := &resources.Notification{
			Content: &resources.Notification_ContentBenchmarkJob{
				ContentBenchmarkJob: &resources.Notification_BenchmarkJobMessage{
					BenchmarkJobId: job.ID,
				},
			},
		}
		notificationPBs[contestant.ID] = notificationPB
	}
	go func() {
		_, err := n.bulkNotify(db, notificationPBs)
		if err != nil {
			log.Println("notify: ", err)
		}
	}()
	return nil
}

func (n *Notifier) SendWebPush(notificationPB *resources.Notification, pushSubscription *PushSubscription) error {
	b, err := proto.Marshal(notificationPB)
	if err != nil {
		return fmt.Errorf("marshal notification: %w", err)
	}
	message := make([]byte, base64.StdEncoding.EncodedLen(len(b)))
	base64.StdEncoding.Encode(message, b)

	vapidPrivateKey := n.VAPIDKey().VAPIDPrivateKey
	vapidPublicKey := n.VAPIDKey().VAPIDPublicKey

	resp, err := webpush.SendNotification(
		message,
		&webpush.Subscription{
			Endpoint: pushSubscription.Endpoint,
			Keys: webpush.Keys{
				Auth:   pushSubscription.Auth,
				P256dh: pushSubscription.P256DH,
			},
		},
		&webpush.Options{
			HTTPClient: client,
			Subscriber:      WebpushSubject,
			VAPIDPublicKey:  vapidPublicKey,
			VAPIDPrivateKey: vapidPrivateKey,
		},
	)
	if err != nil {
		return fmt.Errorf("send notification: %w", err)
	}
	defer resp.Body.Close()
	expired := resp.StatusCode == 410
	if expired {
		return fmt.Errorf("expired notification")
	}
	invalid := resp.StatusCode == 404
	if invalid {
		return fmt.Errorf("invalid notification")
	}
	return nil
}

var PushSubscriptionGroup = &singleflight.Group{}

func getPushSubscriptionsSF(db sqlx.Queryer, contestantID string) ([]PushSubscription, error) {
	v, err, _ := PushSubscriptionGroup.Do(contestantID, func() (interface{}, error) {
		return getPushSubscriptions(db, contestantID)
	})
	if err != nil {
		return nil, err
	}
	return v.([]PushSubscription), nil
}

var PushSubscriptions sync.Map

func getPushSubscriptions(db sqlx.Queryer, contestantID string) ([]PushSubscription, error) {
	val, ok := PushSubscriptions.Load(contestantID)
	if !ok {
		var subscriptions []PushSubscription
		err := sqlx.Select(
			db,
			&subscriptions,
			"SELECT * FROM `push_subscriptions` WHERE `contestant_id` = ?",
			contestantID,
		)
		if err != sql.ErrNoRows && err != nil {
			return nil, fmt.Errorf("select push subscriptions: %w", err)
		}
		PushSubscriptions.Store(contestantID, subscriptions)
		return subscriptions, nil
	}
	return val.([]PushSubscription), nil
}
