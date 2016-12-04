package model

import "testing"

func getTestRecord() []byte {
	return []byte(`
{
  "id":"BizProfile"
  ,"simpleFields":{
    "STATE_MODEL_FACTORY_NAME":"DEFAULT"
    ,"BATCH_MESSAGE_MODE":"false"
    ,"SESSION_ID":"74bb9ce9cd90df5"
    ,"BUCKET_SIZE":"0"
    ,"STATE_MODEL_DEF":"OnlineOffline"
  }
  ,"listFields":{
  }
  ,"mapFields":{
    "partition1":{
      "CURRENT_STATE":"ONLINE"
    }
    ,"partition2":{
      "CURRENT_STATE":"ONLINE"
    }
  }
}
	`)
}

func TestGetMapField(t *testing.T) {
	t.Parallel()

	data := getTestRecord()
	r, err := NewRecordFromBytes(data)
	if err != nil {
		t.Error("panic")
	}

	state := r.GetMapField("partition1", "CURRENT_STATE")
	if state != "ONLINE" {
		t.Error("wrong result")
	}
}

func TestSetMapField(t *testing.T) {
	t.Parallel()

	data := getTestRecord()
	r, err := NewRecordFromBytes(data)
	if err != nil {
		t.Error("panic")
	}

	targetState := "OFFLINE"
	r.SetMapField("partition1", "CURRENT_STATE", targetState)

	state := r.GetMapField("partition1", "CURRENT_STATE")
	if state != targetState {
		t.Error("wrong result")
	}

	// set a new property
	r.SetMapField("partition1", "INFO", "information")
	info := r.GetMapField("partition1", "INFO")
	if info != "information" {
		t.Error("failed")
	}

	// set a new key
	r.SetMapField("partition3", "CURRENT_STATE", "OFFLINE")
	state = r.GetMapField("partition3", "CURRENT_STATE")
	if state != "OFFLINE" {
		t.Error("failed")
	}
}
