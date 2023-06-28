

var QUERY_NAME = "Merkle GA4 Workshop - BigQuery Results";
// Replace this value with your Google Cloud API project ID
var PROJECT_ID = "merkle-taiwan-training";
var DATASET_ID = "ga4_sample_data";
var TABLE_ID = "events_20201130";
if (!PROJECT_ID) throw Error('Project ID is required in setup');

var sheetId = "your google sheet id";
var sheetName = "your google sheet name";


function runQuery() {
  // Replace sample with your own BigQuery query.
  var request = {
    query: "#standardSQL\n" +
    "SELECT COUNT(*) AS cnt, EXTRACT(DATE FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'UTC+8') AS Date, "+
    "EXTRACT(HOUR FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'UTC+8') AS Hour, "+
    "EXTRACT(MINUTE FROM TIMESTAMP_MICROS(event_timestamp) AT TIME ZONE 'UTC+8') AS Minute, "+
    "(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') AS page, "+
    "(SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_title') AS title "+
    "FROM `" + PROJECT_ID + "." + DATASET_ID +"."+TABLE_ID+"` "+
    "WHERE event_name = 'page_view' OR event_name = 'screen_view' "+
    "GROUP BY 2,3,4,5,6 ORDER BY Date, Hour DESC,Minute DESC "
  };
  var queryResults = BigQuery.Jobs.query(request, PROJECT_ID);
  var jobId = queryResults.jobReference.jobId;

  // Wait for BQ job completion (with exponential backoff).
  var sleepTimeMs = 500;
  while (!queryResults.jobComplete) {
    Utilities.sleep(sleepTimeMs);
    sleepTimeMs *= 2;
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId);
  }

  // Get all results from BigQuery.
  var rows = queryResults.rows;
  while (queryResults.pageToken) {
    queryResults = BigQuery.Jobs.getQueryResults(PROJECT_ID, jobId, {
      pageToken: queryResults.pageToken
    });
    rows = rows.concat(queryResults.rows);
  }

  // Return null if no data returned.
  if (!rows) {
    return Logger.log('No rows returned.');
  }

  // Create the new results spreadsheet.
  var spreadsheet = SpreadsheetApp.openById(sheetId);
  var sheet = spreadsheet.getSheetByName(sheetName);
  sheet.clearContents(); // clear

  // Add headers to Sheet.
  var headers = queryResults.schema.fields.map(function(field) {
    return field.name.toUpperCase();
  });
  sheet.appendRow(headers);

  // Append the results.
  var data = new Array(rows.length);
  for (var i = 0; i < rows.length; i++) {
    var cols = rows[i].f;
    data[i] = new Array(cols.length);
    for (var j = 0; j < cols.length; j++) {
      data[i][j] = cols[j].v;
    }
  }

  // Start storing data in row 2, col 1
  var START_ROW = 2;      // skip header row
  var START_COL = 1;
  sheet.getRange(START_ROW, START_COL, rows.length, headers.length).setValues(data);

  Logger.log('Results spreadsheet created: %s', spreadsheet.getUrl());
}