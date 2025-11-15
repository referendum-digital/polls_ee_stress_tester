package main

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

type Log struct {
	UserRequestTimeNanoSeconds int64
	UserRequestSuccess         bool
	VoteRequestTimeNanoSeconds int64
	VoteId                     string
}

type VoteDispatcher struct {
	dispatchChan    chan Request
	LogChan         chan Log
	seed            []byte
	numberOfWorkers int
	baseUrl         string
	pollId          string
	ctx             context.Context
	cancelFunc      context.CancelFunc
	waitGroup       sync.WaitGroup
}

type Request struct {
	UserId string `json:"user_id,omitempty"`
	Type   string `json:"type,omitempty"`
}

func NewVoteDispatcher(seedHex string, baseUrl string, pollId string) *VoteDispatcher {
	ctx, cancel := context.WithCancel(context.Background())
	numberOfWorkers := runtime.NumCPU() * 10
	seed, err := hex.DecodeString(seedHex)
	if err != nil {
		panic(err)
	}
	return &VoteDispatcher{baseUrl: baseUrl, dispatchChan: make(chan Request, numberOfWorkers*4), LogChan: make(chan Log, 100), seed: seed, numberOfWorkers: numberOfWorkers, pollId: pollId, ctx: ctx, cancelFunc: cancel}
}

func (d *VoteDispatcher) Vote(request Request) {
	d.dispatchChan <- request
}

func (d *VoteDispatcher) spawnWorker(ctx context.Context) {
	defer d.waitGroup.Done()
	for {
		select {
		case request := <-d.dispatchChan:
			now := time.Now()
			result, err := sendRequest(ctx, fmt.Sprintf("%s/user", d.baseUrl), d.seed, m{"id": request.UserId, "type": request.Type}, nil)
			userRequestDuration := time.Since(now).Nanoseconds()
			if err != nil {
				log.Printf("Error sending request: %v", err)
				d.LogChan <- Log{
					UserRequestTimeNanoSeconds: userRequestDuration,
					UserRequestSuccess:         false,
				}
				continue
			}
			now = time.Now()
			token := result["token"]
			vote, err := sendRequest(ctx, fmt.Sprintf("%s/poll/%s/vote", d.baseUrl, d.pollId), d.seed, m{"id": request.UserId}, map[string]string{"Authorization": fmt.Sprintf("Bearer %s", token)})
			voteId := ""
			if err == nil {
				voteId = vote["id"].(string)
			}
			voteDuration := time.Since(now).Nanoseconds()
			d.LogChan <- Log{
				UserRequestTimeNanoSeconds: userRequestDuration,
				UserRequestSuccess:         true,
				VoteRequestTimeNanoSeconds: voteDuration,
				VoteId:                     voteId,
			}
		case <-ctx.Done():
			log.Printf("Shutting down hook dispatcher")
			return
		}
	}
}

func (d *VoteDispatcher) Stop() {
	d.cancelFunc()
	// wait for workers to finish
	d.waitGroup.Wait()
	// close the log channel to signal collectors
	close(d.LogChan)
}

func (d *VoteDispatcher) Start() {
	for i := 0; i < d.numberOfWorkers; i++ {
		d.waitGroup.Add(1)
		go d.spawnWorker(d.ctx)
	}
}

func sendRequest(ctx context.Context, url string, seed []byte, body m, headers map[string]string) (map[string]any, error) {
	// Create a new HTTP request
	jsonBytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBytes))
	if err != nil {
		log.Printf("Error creating request: %v", err)
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	ed25519PrivateKey := ed25519.NewKeyFromSeed(seed)

	signature := ed25519.Sign(ed25519PrivateKey, jsonBytes)

	req.Header.Set("sig", hex.EncodeToString(signature))
	req.Header.Set("pk", hex.EncodeToString(ed25519PrivateKey.Public().(ed25519.PublicKey)))

	if headers != nil {
		for k, v := range headers {
			req.Header.Set(k, v)
		}
	}

	// Set up the HTTP client with a timeout
	client := &http.Client{Timeout: 60 * time.Second}

	// Execute the request
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var result map[string]any
	// try to unmarshal response body if possible
	if len(bodyBytes) > 0 {
		if err := json.Unmarshal(bodyBytes, &result); err != nil {
			// if the body is not JSON, still return a helpful error on non-2xx
			if resp.StatusCode != 200 && resp.StatusCode != 201 {
				return nil, fmt.Errorf("status %d: %s", resp.StatusCode, string(bodyBytes))
			}
			// for 2xx with non-json body, return empty map
			result = map[string]any{"raw": string(bodyBytes)}
		}
	} else {
		result = map[string]any{}
	}

	// Success codes: return parsed result
	if resp.StatusCode == 200 || resp.StatusCode == 201 {
		return result, nil
	}

	// Non-success: include body in error
	return nil, fmt.Errorf("non success status code %d: %s", resp.StatusCode, string(bodyBytes))
}

type m map[string]any

func main() {
	baseURl := os.Getenv("BASE_URL")
	if baseURl == "" {
		log.Fatal("BASE_URL environment variable not set")
	}
	signingKeyHex := os.Getenv("SIGNING_KEY")
	if signingKeyHex == "" {
		log.Fatal("SIGNING_KEY environment variable not set")
	}
	signingKey, err := hex.DecodeString(signingKeyHex)
	if err != nil {
		log.Fatalf("failed to decode signing key: %v", err)
	}

	ctx := context.Background()

	totalVotes := 100_000
	blockchainWritingsTotal := 200

	targetValue := totalVotes / blockchainWritingsTotal

	pollDto := m{
		"question": "Who should we send to the space",
		"answers": []m{
			{
				"key":   "anyone",
				"value": "Anyone",
			},
			{
				"key":   "everyone",
				"value": "Everyone",
			},
			{
				"key":   "politicians",
				"value": "Politicians and forget them there",
			},
		},
		"chain_config": m{
			"target_type":  "data_size",
			"target_value": targetValue,
			"write_list": []m{
				{
					"chain":   "ton",
					"network": "testnet",
					"address": "EQDtPeiIAH4QtlHZD8p6_pXoE6iRu3APA8-4RkrXVsEa0PsW",
				},
			},
		},
	}
	newPoll, err := sendRequest(ctx, fmt.Sprintf("%s/poll", baseURl), signingKey, pollDto, nil)
	if err != nil {
		log.Fatalf("failed to send request: %v", err)
	}

	newPollId := newPoll["id"].(string)

	voteDispatcher := NewVoteDispatcher(signingKeyHex, baseURl, newPollId)

	logs := make([]Log, totalVotes)

	voteDispatcher.Start()

	finishedChan := make(chan bool)

	go func() {
		for i := 0; i < totalVotes; i++ {
			l, ok := <-voteDispatcher.LogChan
			if !ok {
				break
			}
			logs[i] = l
		}
		finishedChan <- true
	}()

	for i := 0; i < totalVotes; i++ {
		request := Request{
			UserId: fmt.Sprintf("%d", i+totalVotes),
			Type:   "telegram",
		}
		voteDispatcher.Vote(request)
	}

	select {
	case <-finishedChan:
		// stop dispatcher (waits for workers and closes channels)
		voteDispatcher.Stop()
		for _, logObj := range logs {
			log.Printf("Vote dispatcher finished with %v", logObj)
		}
	}
}
