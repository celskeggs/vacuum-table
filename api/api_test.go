package api

import "testing"

func TestIsAirTableId(t *testing.T) {
	if !IsAirTableId("fldpjJ6SlAbLkrapJ") {
		t.Error("should be an airtable ID")
	}
	if IsAirTableId("fldpjJ6Sl bLkrapJ") {
		t.Error("should not be an airtable ID")
	}
	if IsAirTableId("fldp:J6SlAbLkrapJ") {
		t.Error("should not be an airtable ID")
	}
	if IsAirTableId("fldpjJ6SlAbLkrap^") {
		t.Error("should not be an airtable ID")
	}
	if IsAirTableId("~ldpjJ6SlAbLkrapJ") {
		t.Error("should not be an airtable ID")
	}
}
