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
		err := sqlx.Select(
			db,
			&contestants,
			"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` IS NOT NULL",
		)
		if err != nil {
			return fmt.Errorf("select all contestants: %w", err)
		}
	} else {
		err := sqlx.Select(
			db,
			&contestants,
			"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` = ?",
			c.TeamID,
		)
		if err != nil {
			return fmt.Errorf("select contestants(team_id=%v): %w", c.TeamID, err)
		}
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

	_, err := n.bulkNotify(db, notificationPBs)
	if err != nil {
		return fmt.Errorf("notify: %w", err)
	}
	return nil
}

func (n *Notifier) bulkNotify(db sqlx.Ext, notificationPBs map[string]*resources.Notification) (*Notification, error) {
	values := ""
	for i, v := range notificationPBs {
		m, err := proto.Marshal(v)
		if err != nil {
			return nil, fmt.Errorf("marshal notification: %w", err)
		}
		encodedMessage := base64.StdEncoding.EncodeToString(m)
		values += fmt.Sprintf("(%s, %s, TRUE, NOW(6), NOW(6)),", i, encodedMessage)
	}
	values = values[:len(values)-1]
	query := "INSERT INTO `notifications` (`contestant_id`, `encoded_message`, `read`, `created_at`, `updated_at`) VALUES " + values
	fmt.Println(query)

	res, err := db.Exec(
		query,
	)
	if err != nil {
		return nil, fmt.Errorf("insert notification: %w", err)
	}
	lastInsertID, _ := res.LastInsertId()
	for i, v := range notificationPBs {
		var notification Notification
		err = sqlx.Get(
			db,
			&notification,
			"SELECT * FROM `notifications` WHERE `contestant_id` = ? AND id >= ? LIMIT 1",
			i,
			lastInsertID,
		)
		if err != nil {
			return nil, fmt.Errorf("get inserted notification: %w", err)
		}

		v.Id = notification.ID
		v.CreatedAt = timestamppb.New(notification.CreatedAt)

		subscriptions, err := getPushSubscriptionsSF(db, i)
		if err != nil {
			return nil, fmt.Errorf("get push subscriptions: %w", err)
		}

		go func() {
			for i := 0; i < 3; i++ {
				for _, subscription := range subscriptions {
					err = n.SendWebPush(v, &subscription)
					if err != nil {
						log.Println("send webpush: ", err)
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}()

	}
	return nil, nil
}

func (n *Notifier) NotifyBenchmarkJobFinished(db sqlx.Ext, job *BenchmarkJob) error {
	var contestants []struct {
		ID     string `db:"id"`
		TeamID int64  `db:"team_id"`
	}
	err := sqlx.Select(
		db,
		&contestants,
		"SELECT `id`, `team_id` FROM `contestants` WHERE `team_id` = ?",
		job.TeamID,
	)
	if err != nil {
		return fmt.Errorf("select contestants(team_id=%v): %w", job.TeamID, err)
	}
	for _, contestant := range contestants {
		notificationPB := &resources.Notification{
			Content: &resources.Notification_ContentBenchmarkJob{
				ContentBenchmarkJob: &resources.Notification_BenchmarkJobMessage{
					BenchmarkJobId: job.ID,
				},
			},
		}
		notification, err := n.notify(db, notificationPB, contestant.ID)
		if err != nil {
			return fmt.Errorf("notify: %w", err)
		}
		if n.VAPIDKey() != nil {
			notificationPB.Id = notification.ID
			notificationPB.CreatedAt = timestamppb.New(notification.CreatedAt)
			// TODO: Web Push IIKANJI NI SHITE
		}
	}
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

func getPushSubscriptions(db sqlx.Queryer, contestantID string) ([]PushSubscription, error) {
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
	return subscriptions, nil
}

func (n *Notifier) notify(db sqlx.Ext, notificationPB *resources.Notification, contestantID string) (*Notification, error) {
	m, err := proto.Marshal(notificationPB)
	if err != nil {
		return nil, fmt.Errorf("marshal notification: %w", err)
	}
	encodedMessage := base64.StdEncoding.EncodeToString(m)
	res, err := db.Exec(
		"INSERT INTO `notifications` (`contestant_id`, `encoded_message`, `read`, `created_at`, `updated_at`) VALUES (?, ?, TRUE, NOW(6), NOW(6))",
		contestantID,
		encodedMessage,
	)
	if err != nil {
		return nil, fmt.Errorf("insert notification: %w", err)
	}
	lastInsertID, _ := res.LastInsertId()
	var notification Notification
	err = sqlx.Get(
		db,
		&notification,
		"SELECT * FROM `notifications` WHERE `id` = ? LIMIT 1",
		lastInsertID,
	)
	if err != nil {
		return nil, fmt.Errorf("get inserted notification: %w", err)
	}

	notificationPB.Id = notification.ID
	notificationPB.CreatedAt = timestamppb.New(notification.CreatedAt)

	subscriptions, err := getPushSubscriptionsSF(db, contestantID)
	if err != nil {
		return nil, fmt.Errorf("get push subscriptions: %w", err)
	}

	go func() {
		for i := 0; i < 3; i++ {
			for _, subscription := range subscriptions {
				err = n.SendWebPush(notificationPB, &subscription)
				if err != nil {
					log.Println("send webpush: ", err)
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
	}()

	return &notification, nil
}
