package main

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/joho/godotenv" // Library to read .env files
	_ "github.com/lib/pq"      // The database driver
)

// Incident struct matches the JSON data from the NCDOT feed.
type Incident struct {
	ID                    int     `json:"id" db:"id"`
	Latitude              float64 `json:"latitude" db:"latitude"`
	Longitude             float64 `json:"longitude" db:"longitude"`
	CommonName            string  `json:"commonName" db:"common_name"`
	Reason                string  `json:"reason" db:"reason"`
	Condition             string  `json:"condition" db:"condition"`
	IncidentType          string  `json:"incidentType" db:"incident_type"`
	Severity              int     `json:"severity" db:"severity"`
	Direction             string  `json:"direction" db:"direction"`
	Location              string  `json:"location" db:"location"`
	CountyID              int     `json:"countyId" db:"county_id"`
	CountyName            string  `json:"countyName" db:"county_name"`
	City                  string  `json:"city" db:"city"`
	StartTime             string  `json:"start" db:"start_time"`
	EndTime               string  `json:"end" db:"end_time"`
	LastUpdate            string  `json:"lastUpdate" db:"last_update"`
	Road                  string  `json:"road" db:"road"`
	RouteID               int     `json:"routeId" db:"route_id"`
	LanesClosed           int     `json:"lanesClosed" db:"lanes_closed"`
	LanesTotal            int     `json:"lanesTotal" db:"lanes_total"`
	Detour                string  `json:"detour" db:"detour"`
	CrossStreetPrefix     string  `json:"crossStreetPrefix" db:"cross_street_prefix"`
	CrossStreetNumber     int     `json:"crossStreetNumber" db:"cross_street_number"`
	CrossStreetSuffix     string  `json:"crossStreetSuffix" db:"cross_street_suffix"`
	CrossStreetCommonName string  `json:"crossStreetCommonName" db:"cross_street_common_name"`
	Event                 string  `json:"event" db:"event"`
	CreatedFromConcurrent bool    `json:"createdFromConcurrent" db:"created_from_concurrent"`
	MovableConstruction   string  `json:"movableConstruction" db:"movable_construction"`
	WorkZoneSpeedLimit    int     `json:"workZoneSpeedLimit" db:"work_zone_speed_limit"`
}

type DiscordWebhookBody struct {
	Content string `json:"content"`
}

// ClearedIncident holds just enough info for a cleared notification.
type ClearedIncident struct {
	ID       int
	Road     string
	Location string
	City     string
}

// loadSentIncidents reads the JSON file of sent alert IDs into a map.
func loadSentIncidents(filename string) (map[int]bool, error) {
	sentIDs := make(map[int]bool)
	data, err := os.ReadFile(filename)
	if os.IsNotExist(err) {
		return sentIDs, nil
	} else if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return sentIDs, nil
	}
	err = json.Unmarshal(data, &sentIDs)
	return sentIDs, err
}

// saveSentIncidents writes the updated map of sent alert IDs back to the file.
func saveSentIncidents(filename string, sentIDs map[int]bool) error {
	data, err := json.MarshalIndent(sentIDs, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filename, data, 0644)
}

// sendToDiscord sends a notification for a new vehicle crash.
func sendToDiscord(webhookURL string, incident Incident, formattedTime string) {
	message := fmt.Sprintf(
		"🚨 **Vehicle Crash Alert** 🚨\n\n"+
			"**Road:** %s\n"+
			"**City:** %s\n"+
			"**Location:** %s\n"+
			"**Reason:** %s\n"+
			"**Started:** %s\n"+
			"**Map Link:** [View on Google Maps](https://www.google.com/maps?q=%.6f,%.6f&z=12)",
		incident.Road,
		incident.City,
		incident.Location,
		incident.Reason,
		formattedTime,
		incident.Latitude,
		incident.Longitude,
	)

	payload := DiscordWebhookBody{Content: message}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error creating JSON payload: %s", err)
		return
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error sending to Discord: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Printf("Discord returned non-2xx status: %s", resp.Status)
	}
}

// sendClearedNotificationToDiscord sends an alert when an incident is no longer active.
func sendClearedNotificationToDiscord(webhookURL string, incident ClearedIncident) {
	message := fmt.Sprintf(
		"✅ **Incident Cleared** ✅\n\n"+
			"**Road:** %s\n"+
			"**Location:** %s\n"+
			"**City:** %s",
		incident.Road,
		incident.Location,
		incident.City,
	)

	payload := DiscordWebhookBody{Content: message}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error creating cleared JSON payload: %s", err)
		return
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error sending cleared notification to Discord: %s", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		log.Printf("Discord returned non-2xx status for cleared notification: %s", resp.Status)
	}
}

// upsertIncident inserts a new crash or updates an existing one in the database.
func upsertIncident(db *sql.DB, incident Incident) error {
	sqlStatement := `
		INSERT INTO ncdot_incidents (
			id, latitude, longitude, common_name, reason, "condition", incident_type,
			severity, direction, location, county_id, county_name, city, start_time,
			end_time, last_update, road, route_id, lanes_closed, lanes_total, detour,
			cross_street_prefix, cross_street_number, cross_street_suffix,
			cross_street_common_name, event, created_from_concurrent, movable_construction,
			work_zone_speed_limit, status, cleared_time
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,
			$18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, 'active', NULL
		)
		ON CONFLICT (id) DO UPDATE SET
			latitude = EXCLUDED.latitude,
			longitude = EXCLUDED.longitude,
			reason = EXCLUDED.reason,
			"condition" = EXCLUDED.condition,
			incident_type = EXCLUDED.incident_type,
			severity = EXCLUDED.severity,
			end_time = EXCLUDED.end_time,
			last_update = EXCLUDED.last_update,
			lanes_closed = EXCLUDED.lanes_closed,
			detour = EXCLUDED.detour,
			status = 'active',
			cleared_time = NULL;`

	_, err := db.Exec(sqlStatement,
		incident.ID, incident.Latitude, incident.Longitude, incident.CommonName, incident.Reason,
		incident.Condition, incident.IncidentType, incident.Severity, incident.Direction,
		incident.Location, incident.CountyID, incident.CountyName, incident.City, incident.StartTime,
		incident.EndTime, incident.LastUpdate, incident.Road, incident.RouteID, incident.LanesClosed,
		incident.LanesTotal, incident.Detour, incident.CrossStreetPrefix, incident.CrossStreetNumber,
		incident.CrossStreetSuffix, incident.CrossStreetCommonName, incident.Event,
		incident.CreatedFromConcurrent, incident.MovableConstruction, incident.WorkZoneSpeedLimit,
	)
	return err
}

// clearOldCrashes finds crashes in the DB that are no longer in the feed and marks them cleared.
func clearOldCrashes(db *sql.DB, currentCrashIDs map[int]bool, webhookURL string) error {
	rows, err := db.Query("SELECT id, road, location, city FROM ncdot_incidents WHERE status = 'active' AND incident_type = 'Vehicle Crash'")
	if err != nil {
		return fmt.Errorf("could not query active crashes: %w", err)
	}
	defer rows.Close()

	var activeDbCrashes []ClearedIncident
	for rows.Next() {
		var i ClearedIncident
		if err := rows.Scan(&i.ID, &i.Road, &i.Location, &i.City); err != nil {
			log.Printf("Error scanning active crash from DB: %s", err)
			continue
		}
		activeDbCrashes = append(activeDbCrashes, i)
	}

	var crashesToClear []ClearedIncident
	for _, dbCrash := range activeDbCrashes {
		if !currentCrashIDs[dbCrash.ID] {
			crashesToClear = append(crashesToClear, dbCrash)
		}
	}

	if len(crashesToClear) > 0 {
		log.Printf("Found %d crashes to mark as cleared.", len(crashesToClear))
		for _, crash := range crashesToClear {
			_, err := db.Exec(
				"UPDATE ncdot_incidents SET status = 'cleared', cleared_time = NOW() WHERE id = $1",
				crash.ID,
			)
			if err != nil {
				log.Printf("Error updating crash %d to cleared: %s", crash.ID, err)
			} else {
				log.Printf("Crash %d cleared. Sending notification to Discord.", crash.ID)
				sendClearedNotificationToDiscord(webhookURL, crash)
			}
		}
	} else {
		log.Println("No old crashes to clear.")
	}

	return nil
}

func main() {
	// --- Load .env file ---
	if err := godotenv.Load(); err != nil {
		log.Println("Note: .env file not found, reading credentials from environment")
	}

	// --- Database Connection ---
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=require",
		os.Getenv("DATABASE_HOST"), os.Getenv("DATABASE_PORT"), os.Getenv("DATABASE_USERNAME"),
		os.Getenv("DATABASE_PASSWORD"), os.Getenv("DATABASE_NAME"))

	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		log.Fatalf("Error opening database: %s", err)
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		log.Fatalf("Error connecting to database: %s", err)
	}
	log.Println("Successfully connected to the database.")

	// --- App Setup ---
	url := "https://eapps.ncdot.gov/services/traffic-prod/v1/counties/92/incidents"
	webhookURL := "https://discord.com/api/webhooks/1416378140216922162/4xh5sATlKyECNwEzP05G-Vmg4kGw3XmxsEG8Aezh3tDbW3tD6hfNO5Ev-UOZmJvDQAoR" // IMPORTANT: Replace with your actual webhook URL
	stateFilename := "sent_incidents_ncdot.json"

	sentIDs, err := loadSentIncidents(stateFilename)
	if err != nil {
		log.Fatalf("Error loading sent incidents: %s", err)
	}

	// --- Fetch and Process Data ---
	resp, err := http.Get(url)
	if err != nil {
		log.Fatalf("Error fetching data: %s\n", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Fatalf("Error reading response body: %s\n", err)
	}

	var allIncidents []Incident
	if err := json.Unmarshal(body, &allIncidents); err != nil {
		log.Fatalf("Error unmarshalling JSON: %s\n", err)
	}

	// --- Filter for only Vehicle Crashes ---
	var vehicleCrashes []Incident
	for _, incident := range allIncidents {
		if incident.IncidentType == "Vehicle Crash" {
			vehicleCrashes = append(vehicleCrashes, incident)
		}
	}
	log.Printf("Found %d total incidents, %d of which are vehicle crashes.", len(allIncidents), len(vehicleCrashes))

	currentCrashIDs := make(map[int]bool)
	for _, crash := range vehicleCrashes {
		currentCrashIDs[crash.ID] = true
	}

	log.Println("Processing current vehicle crashes from feed...")
	for _, crash := range vehicleCrashes {
		// Only save vehicle crashes to the database
		if err := upsertIncident(db, crash); err != nil {
			log.Printf("Error upserting crash %d: %s", crash.ID, err)
		}

		// Check if a Discord alert has already been sent for this crash
		if !sentIDs[crash.ID] {
			log.Printf("Found new crash (ID: %d). Sending to Discord...", crash.ID)

			// --- TIMEZONE CONVERSION ---
			loc, err := time.LoadLocation("America/New_York")
			if err != nil {
				log.Printf("Error loading location for timezone conversion: %s", err)
				continue
			}

			parsedTime, err := time.Parse(time.RFC3339, crash.StartTime)
			var formattedTime string
			if err != nil {
				formattedTime = crash.StartTime // Fallback to original string
			} else {
				easternTime := parsedTime.In(loc)
				formattedTime = easternTime.Format("Mon, Jan 2, 3:04 PM EST")
			}

			sendToDiscord(webhookURL, crash, formattedTime)
			sentIDs[crash.ID] = true
		}
	}
	log.Printf("Upserted/updated %d crashes in the database.", len(vehicleCrashes))

	// Check for any crashes that are no longer in the feed
	if err := clearOldCrashes(db, currentCrashIDs, webhookURL); err != nil {
		log.Printf("Error during clearing of old crashes: %s", err)
	}

	// Save the updated list of sent Discord alerts
	if err := saveSentIncidents(stateFilename, sentIDs); err != nil {
		log.Printf("Error saving sent incidents file: %s", err)
	}
	log.Println("Run complete.")
}
