package api

import (
	"bytes"
	"encoding/json"

	"net/http"

	"github.com/proemergotech/machinery/v1/backends/iface"
	"github.com/proemergotech/machinery/v1/common"
	"github.com/proemergotech/machinery/v1/config"
	"github.com/proemergotech/machinery/v1/tasks"
	"gopkg.in/h2non/gentleman.v2"
)

// Backend represents an API result backend
type Backend struct {
	common.Backend
	host string
}

var HTTPClient *gentleman.Client

// New creates Backend instance
func New(cnf *config.Config, host string) iface.Backend {
	return &Backend{
		Backend: common.NewBackend(cnf),
		host:    host,
	}
}

// InitGroup creates and saves a group meta data object
func (b *Backend) InitGroup(groupUUID string, taskUUIDs []string) error {
	// we must implement this function outside of machinery to be able to set workflow ids.
	return nil
}

// GroupCompleted returns true if all tasks in a group finished
func (b *Backend) GroupCompleted(groupUUID string, groupTaskCount int) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	taskStates, err := b.getStates(groupMeta.TaskUUIDs...)
	if err != nil {
		return false, err
	}

	var countSuccessTasks = 0
	for _, taskState := range taskStates {
		if taskState.IsCompleted() {
			countSuccessTasks++
		}
	}

	return countSuccessTasks == groupTaskCount, nil
}

// GroupTaskStates returns states of all tasks in the group
func (b *Backend) GroupTaskStates(groupUUID string, groupTaskCount int) ([]*tasks.TaskState, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return []*tasks.TaskState{}, err
	}

	return b.getStates(groupMeta.TaskUUIDs...)
}

// TriggerChord flags chord as triggered in the backend storage to make sure
// chord is never trigerred multiple times. Returns a boolean flag to indicate
// whether the worker should trigger chord (true) or no if it has been triggered
// already (false)
func (b *Backend) TriggerChord(groupUUID string) (bool, error) {
	groupMeta, err := b.getGroupMeta(groupUUID)
	if err != nil {
		return false, err
	}

	// Chord has already been triggered, return false (should not trigger again)
	if groupMeta.ChordTriggered {
		return false, nil
	}

	data := &map[string]bool{"chord_triggered": true}

	_, err = HTTPClient.
		Request().
		Method(http.MethodPatch).
		Path("api/v1/groups/:group_id").
		Param("group_id", groupUUID).
		JSON(data).
		Do()
	if err != nil {
		return false, err
	}

	return true, nil
}

// SetStatePending updates task state to PENDING
func (b *Backend) SetStatePending(signature *tasks.Signature) error {
	taskState := tasks.NewPendingTaskState(signature)
	return b.updateState(taskState)
}

// SetStateReceived updates task state to RECEIVED
func (b *Backend) SetStateReceived(signature *tasks.Signature) error {
	taskState := tasks.NewReceivedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateStarted updates task state to STARTED
func (b *Backend) SetStateStarted(signature *tasks.Signature) error {
	taskState := tasks.NewStartedTaskState(signature)
	return b.updateState(taskState)
}

// SetStateRetry updates task state to RETRY
func (b *Backend) SetStateRetry(signature *tasks.Signature) error {
	state := tasks.NewRetryTaskState(signature)
	return b.updateState(state)
}

// SetStateSuccess updates task state to SUCCESS
func (b *Backend) SetStateSuccess(signature *tasks.Signature, results []*tasks.TaskResult) error {
	taskState := tasks.NewSuccessTaskState(signature, results)
	return b.updateState(taskState)
}

// SetStateFailure updates task state to FAILURE
func (b *Backend) SetStateFailure(signature *tasks.Signature, err string) error {
	taskState := tasks.NewFailureTaskState(signature, err)
	return b.updateState(taskState)
}

// GetState returns the latest task state
func (b *Backend) GetState(taskUUID string) (*tasks.TaskState, error) {
	resp, err := HTTPClient.
		Request().
		Method(http.MethodPost).
		Path("/api/v1/tasks/:task_id").
		Param("task_id", taskUUID).
		Do()
	if err != nil {
		return nil, err
	}

	var taskState *tasks.TaskState
	decoder := json.NewDecoder(bytes.NewReader(resp.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(taskState); err != nil {
		return nil, err
	}

	return taskState, nil
}

// PurgeState deletes stored task state
func (b *Backend) PurgeState(taskUUID string) error {
	// not implemented

	return nil
}

// PurgeGroupMeta deletes stored group meta data
func (b *Backend) PurgeGroupMeta(groupUUID string) error {
	// not implemented

	return nil
}

// getGroupMeta retrieves group meta data, convenience function to avoid repetition
func (b *Backend) getGroupMeta(groupUUID string) (*tasks.GroupMeta, error) {
	resp, err := HTTPClient.
		Request().
		Method(http.MethodGet).
		Path("api/v1/groups/:group_id").
		Param("group_id", groupUUID).
		Do()
	if err != nil {
		return nil, err
	}

	groupMeta := new(tasks.GroupMeta)
	decoder := json.NewDecoder(bytes.NewReader(resp.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(groupMeta); err != nil {
		return nil, err
	}

	return groupMeta, nil
}

// getStates returns multiple task states
func (b *Backend) getStates(taskUUIDs ...string) ([]*tasks.TaskState, error) {
	data := &map[string][]string{"task_uuids": taskUUIDs}

	resp, err := HTTPClient.
		Request().
		Method(http.MethodPost).
		Path("/api/v1/tasks").
		JSON(data).
		Do()
	if err != nil {
		return nil, err
	}

	var taskStates []*tasks.TaskState
	decoder := json.NewDecoder(bytes.NewReader(resp.Bytes()))
	decoder.UseNumber()
	if err := decoder.Decode(taskStates); err != nil {
		return nil, err
	}

	return taskStates, nil
}

// updateState saves current task state
func (b *Backend) updateState(taskState *tasks.TaskState) error {
	_, err := HTTPClient.
		Request().
		Method(http.MethodPatch).
		Path("api/v1/tasks/:task_id").
		Param("task_id", taskState.TaskUUID).
		Do()
	if err != nil {
		return err
	}

	return nil
}

// setExpirationTime sets expiration timestamp on a stored task state
func (b *Backend) setExpirationTime(key string) error {
	// not implemented

	return nil
}

// client returns or creates instance of HTTP client
func (b *Backend) client() *gentleman.Client {
	if HTTPClient == nil {
		HTTPClient = gentleman.New().BaseURL(b.host)
	}
	return HTTPClient
}
