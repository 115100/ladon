package ladon

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
	"gopkg.in/redis.v5"
)

// Copy of rdbSchema hack
type redisSchema struct {
	ID          string          `json:"id"`
	Description string          `json:"description"`
	Subjects    []string        `json:"subjects"`
	Effect      string          `json:"effect"`
	Resources   []string        `json:"resources"`
	Actions     []string        `json:"actions"`
	Conditions  json.RawMessage `json:"conditions"`
}

// RedisManager is a redis implementation of Manager to store policies persistently.
type RedisManager struct {
	db *redis.Client
}

// NewRedisManager initializes a new RedisManager with no policies
func NewRedisManager(db *redis.Client) *RedisManager {
	return &RedisManager{
		db: db,
	}
}

const redisPolicyTemplate = "ladon:policy:%s"

func redisPolicyID(id string) string {
	return fmt.Sprintf(redisPolicyTemplate, id)
}

// Create a new policy to RedisManager
func (m *RedisManager) Create(policy Policy) error {
	policyID := redisPolicyID(policy.GetID())

	if exists := m.db.Get(policyID).Err(); exists != redis.Nil {
		return errors.New("Policy exists")
	}

	payload, err := json.Marshal(policy)
	if err != nil {
		return err
	}

	return m.db.Set(policyID, payload, 0).Err()
}

func (m *RedisManager) getPolicyFromRedis(key string) (Policy, error) {
	s := new(redisSchema)

	resp, err := m.db.Get(key).Bytes()
	if err == redis.Nil {
		return nil, errors.New("Not found")
	} else if err != nil {
		return nil, errors.Wrap(err, "")
	}

	if err := json.Unmarshal(resp, s); err != nil {
		return nil, errors.Wrap(err, "")
	}

	p := &DefaultPolicy{
		ID:          s.ID,
		Description: s.Description,
		Subjects:    s.Subjects,
		Effect:      s.Effect,
		Resources:   s.Resources,
		Actions:     s.Actions,
		Conditions:  Conditions{},
	}

	if err := p.Conditions.UnmarshalJSON(s.Conditions); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return p, nil
}

// Get retrieves a policy.
func (m *RedisManager) Get(id string) (Policy, error) {
	return m.getPolicyFromRedis(redisPolicyID(id))
}

// Delete removes a policy.
func (m *RedisManager) Delete(id string) error {
	return m.db.Del(redisPolicyID(id)).Err()
}

// FindPoliciesForSubject finds all policies associated with the subject.
func (m *RedisManager) FindPoliciesForSubject(subject string) (Policies, error) {
	ps := Policies{}

	iter := m.db.Scan(0, redisPolicyID("*"), 0).Iterator()
	for iter.Next() {
		pk := iter.Val()

		p, err := m.getPolicyFromRedis(pk)
		if err != nil {
			return nil, errors.Wrap(err, "")
		}

		if ok, err := Match(p, p.GetSubjects(), subject); err != nil {
			return nil, err
		} else if !ok {
			continue
		}

		ps = append(ps, p)
	}
	if err := iter.Err(); err != nil {
		return nil, errors.Wrap(err, "")
	}

	return ps, nil
}
