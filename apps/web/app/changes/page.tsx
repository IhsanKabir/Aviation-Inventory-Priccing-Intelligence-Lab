import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import {
  getAirlines,
  getChangeEventsPayload,
  getRoutes
} from "@/lib/api";
import { formatDhakaDateTime, formatNumber, formatPublicValue } from "@/lib/format";
import { firstParam, manyParams, parseLimit, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

function selectedRouteKey(origin?: string, destination?: string) {
  if (!origin || !destination) {
    return undefined;
  }
  return `${origin}-${destination}`;
}

function renderCompactValue(value: unknown) {
  return formatPublicValue(value);
}

const HIDDEN_FIELD_NAMES = new Set([
  "scraped_at",
  "source_endpoint",
  "raw_offer",
  "ota_name",
  "penalty_source",
  "fare_search_signature",
  "fare_search_reference",
  "fare_ref_num"
]);

function toDisplayFieldName(value?: string | null) {
  if (!value) {
    return "-";
  }

  const explicitLabels: Record<string, string> = {
    tax_amount: "Tax amount",
    total_price_bdt: "Total price",
    base_fare_amount: "Base fare",
    ota_gross_fare: "Channel gross fare",
    ota_discount_amount: "Channel discount amount",
    ota_discount_pct: "Channel discount percent",
    seat_available: "Seat available",
    seat_capacity: "Seat capacity",
    load_factor_pct: "Load factor",
    booking_class: "Booking class",
    penalty_rule_text: "Penalty text",
    operating_airline: "Operating airline"
  };

  if (explicitLabels[value]) {
    return explicitLabels[value];
  }

  return value
    .split("_")
    .filter(Boolean)
    .map((token) => token.charAt(0).toUpperCase() + token.slice(1))
    .join(" ");
}

export default async function ChangesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const selectedDomains = manyParams(params, "domain");
  const selectedChangeTypes = manyParams(params, "change_type");
  const selectedDirections = manyParams(params, "direction");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const startDate = firstParam(params, "start_date");
  const endDate = firstParam(params, "end_date");
  const limit = parseLimit(firstParam(params, "limit"), 150);
  const routeKey = selectedRouteKey(origin, destination);

  const [airlines, routes, changes] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getChangeEventsPayload({
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      domains: selectedDomains,
      changeTypes: selectedChangeTypes,
      directions: selectedDirections,
      startDate,
      endDate,
      limit
    })
  ]);

  const rawRows = changes.data?.items ?? [];
  const rows = rawRows.filter((row) => !HIDDEN_FIELD_NAMES.has((row.field_name ?? "").trim()));
  const airlineOptions = [...(airlines.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline))
    .slice(0, 20)
    .map((item) => item.airline);
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));

  const routeCount = new Set(rows.map((row) => row.route_key).filter(Boolean)).size;
  const airlineCount = new Set(rows.map((row) => row.airline)).size;
  const upCount = rows.filter((row) => row.direction === "up").length;
  const downCount = rows.filter((row) => row.direction === "down").length;

  return (
    <>
      <h1 className="page-title">Change Event Browser</h1>
      <p className="page-copy">
        Field-level event browser for fare, inventory, schedule, penalty, and tax movement.
        This screen replaces workbook-style history scanning with direct event queries.
      </p>

      <div className="grid cards">
        <MetricCard label="Events" value={rows.length.toLocaleString()} footnote={`Limit ${limit.toLocaleString()}`} />
        <MetricCard label="Routes" value={routeCount.toLocaleString()} footnote={routeKey ? routeKey : "All routes in scope"} />
        <MetricCard label="Airlines" value={airlineCount.toLocaleString()} footnote={selectedAirlines.length ? `${selectedAirlines.length} selected` : "All carriers"} />
        <MetricCard label="Direction split" value={`${upCount.toLocaleString()} / ${downCount.toLocaleString()}`} footnote="Up vs down events" />
      </div>

      <div className="stack">
        <DataPanel
          title="Event filters"
          copy="Route and airline chips update immediately. Domain, change-type, and direction chips narrow the exact event set."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={[
              "airline",
              "origin",
              "destination",
              "domain",
              "change_type",
              "direction",
              "start_date",
              "end_date",
              "limit"
            ]}
            extraGroups={[
              {
                key: "domain",
                label: "Domains",
                selected: selectedDomains,
                options: [
                  { label: "Price", value: "price" },
                  { label: "Availability", value: "availability" },
                  { label: "Capacity", value: "capacity" },
                  { label: "Schedule", value: "schedule" },
                  { label: "Seat", value: "seat" },
                  { label: "Field", value: "field" }
                ]
              },
              {
                key: "change_type",
                label: "Change types",
                selected: selectedChangeTypes,
                options: [
                  { label: "Increase", value: "increase" },
                  { label: "Decrease", value: "decrease" },
                  { label: "Added", value: "added" },
                  { label: "Removed", value: "removed" }
                ]
              },
              {
                key: "direction",
                label: "Directions",
                selected: selectedDirections,
                options: [
                  { label: "Up", value: "up" },
                  { label: "Down", value: "down" },
                  { label: "None", value: "none" }
                ]
              }
            ]}
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              start_date: startDate ?? "",
              end_date: endDate ?? "",
              limit: String(limit)
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "RUH" },
              { name: "start_date", label: "Start date", type: "date" },
              { name: "end_date", label: "End date", type: "date" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" }
            ]}
            routeOptions={routeOptions}
            selectedAirlines={selectedAirlines}
            selectedRouteKey={routeKey}
          />
        </DataPanel>

        <DataPanel
          title="Event rows"
          copy={routeKey ? `Showing field-level changes for ${routeKey}.` : "Showing field-level changes across the selected operational scope."}
        >
          {!changes.ok ? (
            <div className="empty-state error-state">API error: {changes.error ?? "Unable to load change events."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No change events matched the current filter set.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table compact-table">
                <thead>
                  <tr>
                    <th>Detected</th>
                    <th>Route</th>
                    <th>Airline</th>
                    <th>Flight</th>
                    <th>Domain</th>
                    <th>Field</th>
                    <th>Type</th>
                    <th>Direction</th>
                    <th>Old value</th>
                    <th>New value</th>
                    <th>Magnitude</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, index) => (
                    <tr key={`${row.id}-${row.detected_at_utc ?? ""}-${index}`}>
                      <td>{formatDhakaDateTime(row.detected_at_utc)}</td>
                      <td>{row.route_key ?? "-"}</td>
                      <td>{row.airline}</td>
                      <td>
                        <div className="table-cell-stack">
                          <strong>{row.flight_number ?? "-"}</strong>
                          <span>{row.departure_time ?? "-"}</span>
                        </div>
                      </td>
                      <td>{row.domain ?? "-"}</td>
                      <td>{toDisplayFieldName(row.field_name)}</td>
                      <td>{row.change_type ?? "-"}</td>
                      <td>{row.direction ?? "-"}</td>
                      <td className="long-text">{renderCompactValue(row.old_value)}</td>
                      <td className="long-text">{renderCompactValue(row.new_value)}</td>
                      <td>
                        <div className="table-cell-stack">
                          <span>{formatNumber(row.magnitude)}</span>
                          <span>{row.percent_change !== null && row.percent_change !== undefined ? `${row.percent_change.toFixed(2)}%` : "-"}</span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </DataPanel>
      </div>
    </>
  );
}
