import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import {
  getAirlines,
  getChangeDashboardPayload,
  getChangeEventsPayload,
  getRoutes
} from "@/lib/api";
import { buildReportingExportUrl } from "@/lib/export";
import { formatDhakaDateTime, formatNumber, formatPublicValue, formatRouteGeo, formatRouteType } from "@/lib/format";
import { buildHref, firstParam, manyParams, parseLimit, removeParams, setParam, type RawSearchParams } from "@/lib/query";

type PageProps = {
  searchParams?: Promise<RawSearchParams>;
};

const WINDOW_OFFSETS: Record<string, number> = {
  today: 0,
  last_3d: 2,
  last_7d: 6,
  last_14d: 13
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

function formatIsoDate(value: Date) {
  return value.toISOString().slice(0, 10);
}

function offsetIsoDate(value: string, deltaDays: number) {
  const next = new Date(`${value}T00:00:00.000Z`);
  next.setUTCDate(next.getUTCDate() + deltaDays);
  return formatIsoDate(next);
}

function resolveDateWindow(startDate?: string, endDate?: string, windowKey?: string) {
  if (startDate || endDate) {
    return {
      startDate,
      endDate
    };
  }

  const offset = windowKey ? WINDOW_OFFSETS[windowKey] : undefined;
  if (offset === undefined) {
    return {
      startDate,
      endDate
    };
  }

  const today = formatIsoDate(new Date());
  return {
    startDate: offsetIsoDate(today, -offset),
    endDate: today
  };
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

function formatDayLabel(value?: string | null) {
  if (!value) {
    return "-";
  }

  const dateValue = new Date(`${value}T00:00:00.000Z`);
  return new Intl.DateTimeFormat("en-GB", {
    day: "2-digit",
    month: "short"
  }).format(dateValue);
}

export default async function ChangesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const selectedDomains = manyParams(params, "domain");
  const selectedChangeTypes = manyParams(params, "change_type");
  const selectedDirections = manyParams(params, "direction");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const explicitStartDate = firstParam(params, "start_date");
  const explicitEndDate = firstParam(params, "end_date");
  const selectedWindow = firstParam(params, "window");
  const limit = parseLimit(firstParam(params, "limit"), 150);
  const topN = 8;
  const routeKey = selectedRouteKey(origin, destination);
  const { startDate, endDate } = resolveDateWindow(explicitStartDate, explicitEndDate, selectedWindow);

  const [airlines, routes, dashboard, changes] = await Promise.all([
    getAirlines(),
    getRoutes(),
    getChangeDashboardPayload({
      airlines: selectedAirlines,
      origins: origin ? [origin] : undefined,
      destinations: destination ? [destination] : undefined,
      domains: selectedDomains,
      changeTypes: selectedChangeTypes,
      directions: selectedDirections,
      startDate,
      endDate,
      topN
    }),
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

  const summary = dashboard.data?.summary;
  const totalEvents = summary?.event_count ?? rows.length;
  const routeCount = summary?.route_count ?? new Set(rows.map((row) => row.route_key).filter(Boolean)).size;
  const airlineCount = summary?.airline_count ?? new Set(rows.map((row) => row.airline)).size;
  const upCount = summary?.up_count ?? rows.filter((row) => row.direction === "up").length;
  const downCount = summary?.down_count ?? rows.filter((row) => row.direction === "down").length;
  const addedCount = summary?.added_count ?? rows.filter((row) => row.change_type === "added").length;
  const removedCount = summary?.removed_count ?? rows.filter((row) => row.change_type === "removed").length;
  const latestEventAt = summary?.latest_event_at_utc ?? rows[0]?.detected_at_utc ?? null;
  const topRoutes = dashboard.data?.top_routes ?? [];
  const topAirlines = dashboard.data?.top_airlines ?? [];
  const topFields = dashboard.data?.field_mix ?? [];
  const topDomains = dashboard.data?.domain_mix ?? [];
  const biggestMoves = (dashboard.data?.largest_moves ?? rows)
    .filter((row) => row.route_key && row.airline)
    .slice(0, topN);
  const dailySeries = dashboard.data?.daily_series ?? [];
  const maxDailyEvents = dailySeries.reduce((currentMax, item) => Math.max(currentMax, item.event_count ?? 0), 0);
  const exportHref = buildReportingExportUrl(params, ["changes"]);

  return (
    <>
      <h1 className="page-title">Changes</h1>
      <p className="page-copy">
        Market movement dashboard for fare, inventory, schedule, penalty, and tax changes.
        The top layer is optimized for scanning; the full event table remains available for row-level verification.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Events"
          value={totalEvents.toLocaleString()}
          footnote={`${rows.length.toLocaleString()} visible rows, limit ${limit.toLocaleString()}`}
        />
        <MetricCard label="Routes" value={routeCount.toLocaleString()} footnote={routeKey ? routeKey : "All routes in scope"} />
        <MetricCard label="Airlines" value={airlineCount.toLocaleString()} footnote={selectedAirlines.length ? `${selectedAirlines.length} selected` : "All carriers"} />
        <MetricCard label="Direction split" value={`${upCount.toLocaleString()} / ${downCount.toLocaleString()}`} footnote="Up vs down events" />
      </div>

      <div className="section-grid changes-grid">
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
              "window",
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

          <div className="filter-group">
            <div className="filter-label">History windows</div>
            <div className="chip-row">
              {[
                { label: "Today", value: "today" },
                { label: "Last 3d", value: "last_3d" },
                { label: "Last 7d", value: "last_7d" },
                { label: "Last 14d", value: "last_14d" }
              ].map((item) => (
                <a
                  className="chip"
                  data-active={!explicitStartDate && !explicitEndDate && selectedWindow === item.value}
                  href={buildHref(setParam(removeParams(params, ["start_date", "end_date"]), "window", item.value))}
                  key={item.value}
                >
                  {item.label}
                </a>
              ))}
              <a
                className="chip"
                data-active={!explicitStartDate && !explicitEndDate && !selectedWindow}
                href={buildHref(removeParams(params, ["window", "start_date", "end_date"]))}
              >
                All history
              </a>
            </div>
          </div>

          <div className="button-row">
            <a className="button-link ghost" href={exportHref}>
              Download Excel
            </a>
          </div>
        </DataPanel>

        <DataPanel
          title="Market pulse"
          copy={
            routeKey
              ? `Showing market movement summaries for ${routeKey}.`
              : "Showing market movement summaries across the selected operational scope."
          }
        >
          {!dashboard.ok ? (
            <div className="empty-state error-state">API error: {dashboard.error ?? "Unable to load dashboard summary."}</div>
          ) : totalEvents === 0 ? (
            <div className="empty-state">No change events matched the current filter set.</div>
          ) : (
            <div className="changes-summary-stack">
              <div className="status-banner">
                <strong>Latest movement:</strong> {formatDhakaDateTime(latestEventAt)}.{" "}
                <span>
                  {upCount.toLocaleString()} up, {downCount.toLocaleString()} down, {addedCount.toLocaleString()} added, {removedCount.toLocaleString()} removed.
                </span>
              </div>

              <div className="changes-trend-list">
                <div className="filter-label">Daily movement</div>
                {dailySeries.length === 0 ? (
                  <div className="empty-state">No daily change history in the selected scope.</div>
                ) : (
                  dailySeries.map((item) => {
                    const width = maxDailyEvents > 0 ? Math.max(10, Math.round((item.event_count / maxDailyEvents) * 100)) : 0;
                    return (
                      <div className="changes-trend-row" key={item.report_day}>
                        <strong>{formatDayLabel(item.report_day)}</strong>
                        <div className="changes-trend-bar-track">
                          <div className="changes-trend-bar" style={{ width: `${width}%` }} />
                        </div>
                        <span>{item.event_count.toLocaleString()} events</span>
                        <span>
                          {item.up_count ?? 0} up / {item.down_count ?? 0} down
                        </span>
                      </div>
                    );
                  })
                )}
              </div>

              <div className="changes-summary-grid">
                <div className="table-list">
                  <div className="filter-label">Most affected routes</div>
                  {topRoutes.map((item) => (
                    <div className="table-row" key={`route-${item.route_key}`}>
                      <strong>{item.route_key}</strong>
                      <span>
                        {item.airline_count ?? 0} airlines, latest {formatDhakaDateTime(item.latest_event_at_utc)}
                      </span>
                      <span className="pill warn">{item.event_count.toLocaleString()}</span>
                    </div>
                  ))}
                </div>

                <div className="table-list">
                  <div className="filter-label">Most active airlines</div>
                  {topAirlines.map((item) => (
                    <div className="table-row" key={`airline-${item.airline}`}>
                      <strong>{item.airline}</strong>
                      <span>
                        {item.route_count ?? 0} routes, latest {formatDhakaDateTime(item.latest_event_at_utc)}
                      </span>
                      <span className="pill good">{item.event_count.toLocaleString()}</span>
                    </div>
                  ))}
                </div>
              </div>

              <div className="changes-summary-grid">
                <div className="table-list">
                  <div className="filter-label">Field mix</div>
                  {topFields.map((item) => (
                    <div className="table-row" key={`field-${item.field_name ?? item.display_name}`}>
                      <strong>{item.display_name}</strong>
                      <span>Most frequently changed fields</span>
                      <span>{item.event_count.toLocaleString()}</span>
                    </div>
                  ))}
                </div>

                <div className="table-list">
                  <div className="filter-label">Domain mix</div>
                  {topDomains.map((item) => (
                    <div className="table-row" key={`domain-${item.domain}`}>
                      <strong>{item.domain}</strong>
                      <span>Change domain share</span>
                      <span>{item.event_count.toLocaleString()}</span>
                    </div>
                  ))}
                </div>
              </div>
            </div>
          )}
        </DataPanel>
      </div>

      <div className="stack">
        <DataPanel
          title="Event rows"
          copy={routeKey ? `Showing field-level changes for ${routeKey}.` : "Showing field-level changes across the selected operational scope."}
        >
          {!changes.ok ? (
            <div className="empty-state error-state">API error: {changes.error ?? "Unable to load change events."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No change events matched the current filter set.</div>
          ) : (
            <>
              <div className="changes-callout-row">
                <div className="filter-group">
                  <div className="filter-label">Largest visible moves</div>
                  <div className="chip-row">
                    {biggestMoves.map((row, index) => (
                      <span className="chip change-summary-chip" key={`${row.id}-${index}`}>
                        {row.route_key} {row.airline} {toDisplayFieldName(row.field_name)} {formatNumber(row.magnitude)}
                      </span>
                    ))}
                  </div>
                </div>
              </div>

              <div className="data-table-wrap">
                <table className="data-table compact-table change-table">
                  <thead>
                    <tr>
                      <th className="sticky-change-col">Detected</th>
                      <th className="sticky-change-col second">Route</th>
                      <th className="sticky-change-col third">Airline</th>
                      <th className="sticky-change-col fourth">Flight</th>
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
                        <td className="sticky-change-col change-table-meta">{formatDhakaDateTime(row.detected_at_utc)}</td>
                        <td className="sticky-change-col second change-table-meta">
                          <div className="table-cell-stack">
                            <strong>{row.route_key ?? "-"}</strong>
                            <span className="route-inline-meta">
                              <span className="route-type-pill" data-type={formatRouteType(row.route_type)}>
                                {formatRouteType(row.route_type)}
                              </span>
                              <span>{formatRouteGeo(row.origin_country_code, row.destination_country_code)}</span>
                            </span>
                          </div>
                        </td>
                        <td className="sticky-change-col third change-table-meta">{row.airline}</td>
                        <td className="sticky-change-col fourth change-table-meta">
                          <div className="table-cell-stack">
                            <strong>{row.flight_number ?? "-"}</strong>
                            <span>{row.departure_time ?? "-"}</span>
                          </div>
                        </td>
                        <td><span className="change-pill">{row.domain ?? "-"}</span></td>
                        <td>{toDisplayFieldName(row.field_name)}</td>
                        <td><span className="change-pill">{row.change_type ?? "-"}</span></td>
                        <td>
                          <span className={`change-pill direction-${row.direction ?? "neutral"}`}>
                            {row.direction ?? "-"}
                          </span>
                        </td>
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
            </>
          )}
        </DataPanel>
      </div>
    </>
  );
}
