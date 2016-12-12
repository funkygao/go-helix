package model

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/funkygao/go-helix/ver"
)

// Generic Record Format to store data at a storage Node.
// simpleFields mapFields listFields.
type Record struct {
	ID string `json:"id"`

	// plain key, value fields
	SimpleFields map[string]interface{} `json:"simpleFields"`

	// all fields whose values are a list of values
	ListFields map[string][]string `json:"listFields"`

	// all fields whose values are key, value
	MapFields map[string]map[string]string `json:"mapFields"`
}

// NewRecord creates a new instance of Record instance
func NewRecord(id string) *Record {
	return &Record{
		ID:           id,
		SimpleFields: map[string]interface{}{},
		ListFields:   map[string][]string{},
		MapFields:    map[string]map[string]string{},
	}
}

func (r Record) BucketSize() int {
	return r.GetIntField("BUCKET_SIZE", 0)
}

func (r *Record) SetBucketSize(size int) {
	if size < 0 {
		size = 0
	}
	r.SetIntField("BUCKET_SIZE", size)
}

func (r Record) BatchMessageMode() bool {
	return r.GetBooleanField("BATCH_MESSAGE_MODE", false)
}

func (r *Record) SetBatchMessageMode(yes bool) {
	r.SetBooleanField("BATCH_MESSAGE_MODE", yes)
}

// GetSimpleField returns a value of a key in SimpleField structure
func (r Record) GetSimpleField(key string) interface{} {
	if r.SimpleFields == nil {
		return nil
	}

	return r.SimpleFields[key]
}

// GetStringField returns the string value of a field in the SimpleField
func (r Record) GetStringField(key string, defaultValue string) string {
	value := r.GetSimpleField(key)
	if value == nil {
		return defaultValue
	}

	strVal, ok := value.(string)
	if !ok {
		return defaultValue
	}
	return strVal
}

func (r Record) SetStringField(key string, value string) {
	r.SetSimpleField(key, value)
}

// GetIntField returns the integer value of a field in the SimpleField
func (r Record) GetIntField(key string, defaultValue int) int {
	value := r.GetSimpleField(key)
	if value == nil {
		return defaultValue
	}

	intVal, err := strconv.Atoi(value.(string))
	if err != nil {
		return defaultValue
	}
	return intVal
}

// SetIntField sets the integer value of a key under SimpleField.
// the value is stored as in string form
func (r *Record) SetIntField(key string, value int) {
	r.SetSimpleField(key, strconv.Itoa(value))
}

// GetBooleanField gets the value of a key under SimpleField and
// convert the result to bool type. That is, if the value is "true",
// the result is true.
func (r Record) GetBooleanField(key string, defaultValue bool) bool {
	result := r.GetSimpleField(key)
	if result == nil {
		return defaultValue
	}

	return strings.ToLower(result.(string)) == "true"
}

// SetBooleanField sets a key under SimpleField with a specified bool
// value, serialized to string. For example, true will be stored as
// "TRUE"
func (r *Record) SetBooleanField(key string, value bool) {
	r.SetSimpleField(key, strconv.FormatBool(value))
}

func (r *Record) GetListField(key string) []string {
	return r.ListFields[key]
}

func (r *Record) AddListField(key string, value string) {
	values, present := r.ListFields[key]
	if !present {
		values = []string{value}
	} else {
		values = append(values, value)
	}
	r.ListFields[key] = values
}

func (r *Record) RemoveListField(key string, value string) {
	values, present := r.ListFields[key]
	if !present {
		return
	}

	newValues := make([]string, 0, len(values))
	for _, v := range values {
		if v != value {
			newValues = append(newValues, v)
		}
	}
	r.ListFields[key] = newValues
}

// SetSimpleField sets the value of a key under SimpleField
func (r *Record) SetSimpleField(key string, value interface{}) {
	if r.SimpleFields == nil {
		r.SimpleFields = make(map[string]interface{})
	}
	r.SimpleFields[key] = value
}

// GetMapField returns the string value of the property of a key under MapField.
func (r Record) GetMapField(key string, property string) string {
	if r.MapFields == nil || r.MapFields[key] == nil || r.MapFields[key][property] == "" {
		return ""
	}

	return r.MapFields[key][property]
}

// SetMapField sets the value of a key under MapField. Both key and
// value are string format.
func (r *Record) SetMapField(key string, property string, value string) {
	if r.MapFields == nil {
		r.MapFields = make(map[string]map[string]string)
	}

	if r.MapFields[key] == nil {
		r.MapFields[key] = make(map[string]string)
	}

	r.MapFields[key][property] = value
}

// RemoveMapField deletes a key from MapField
func (r *Record) RemoveMapField(key string) {
	if r.MapFields == nil || r.MapFields[key] == nil {
		return
	}

	delete(r.MapFields, key)
}

// Marshal generates the beautified json in byte array format
func (r Record) Marshal() []byte {
	b, _ := json.MarshalIndent(r, "", "    ") // ignore the err, should never happen
	return b
}

// FIXME
func (r1 Record) Equals(r2 *Record) bool {
	if len(r1.SimpleFields) != len(r2.SimpleFields) {
		return false
	}
	if len(r1.ListFields) != len(r2.ListFields) {
		return false
	}
	if len(r1.MapFields) != len(r2.MapFields) {
		return false
	}

	/*
		if r1.SimpleFields != r2.SimpleFields {
			return false
		}
		if r1.ListFields != r2.ListFields {
			return false
		}
		if r1.MapFields != r2.MapFields {
			return false
		}*/

	return true
}

// String returns the beautified JSON string for the Record
func (r Record) String() string {
	return string(r.Marshal())
}

// NewRecordFromBytes creates a new znode instance from a byte array
func NewRecordFromBytes(data []byte) (*Record, error) {
	var zn Record
	err := json.Unmarshal(data, &zn)
	return &zn, err
}

// NewLiveInstanceRecord creates a new instance of Record for representing a live instance.
func NewLiveInstanceRecord(participantID string, sessionID string) *Record {
	hostname, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	node := NewRecord(participantID)
	node.SetSimpleField("HELIX_VERSION", ver.Ver)
	node.SetSimpleField("SESSION_ID", sessionID)
	node.SetSimpleField("LIVE_INSTANCE", fmt.Sprintf("%d@%s", os.Getpid(), hostname))

	return node
}
