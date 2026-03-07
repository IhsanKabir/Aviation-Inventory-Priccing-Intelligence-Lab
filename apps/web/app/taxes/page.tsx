import { LiveFilterControls } from "@/components/live-filter-controls";
import { DataPanel } from "@/components/data-panel";
import { MetricCard } from "@/components/metric-card";
import { getAirlines, getLatestCycle, getRoutes, getTaxPayload } from "@/lib/api";
import { formatDhakaDateTime, formatMoney, shortCycle } from "@/lib/format";
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

export default async function TaxesPage({ searchParams }: PageProps) {
  const params = (await searchParams) ?? {};
  const selectedAirlines = manyParams(params, "airline");
  const origin = firstParam(params, "origin");
  const destination = firstParam(params, "destination");
  const limit = parseLimit(firstParam(params, "limit"), 120);
  const routeKey = selectedRouteKey(origin, destination);

  const [latestCycle, airlines, routes] = await Promise.all([
    getLatestCycle(),
    getAirlines(),
    getRoutes()
  ]);

  const cycleId = firstParam(params, "cycle_id") ?? latestCycle.data?.cycle_id ?? undefined;
  const taxes = await getTaxPayload({
    cycleId,
    airlines: selectedAirlines,
    origins: origin ? [origin] : undefined,
    destinations: destination ? [destination] : undefined,
    limit
  });

  const rows = taxes.data?.rows ?? [];
  const airlineOptions = [...(airlines.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.airline.localeCompare(right.airline))
    .slice(0, 20)
    .map((item) => item.airline);
  const routeOptions = [...(routes.data?.items ?? [])]
    .sort((left, right) => (right.offer_rows ?? 0) - (left.offer_rows ?? 0) || left.route_key.localeCompare(right.route_key))
    .slice(0, 16)
    .map((item) => ({ routeKey: item.route_key, origin: item.origin, destination: item.destination }));

  const routeCount = new Set(rows.map((row) => row.route_key)).size;
  const airlineCount = new Set(rows.map((row) => row.airline)).size;
  const maxTax = rows.reduce((current, row) => Math.max(current, row.tax_amount ?? 0), 0);

  return (
    <>
      <h1 className="page-title">Tax Reference</h1>
      <p className="page-copy">
        Current-cycle tax view for route and airline comparison. This replaces the
        workbook-only tax sheet with a direct operational screen.
      </p>

      <div className="grid cards">
        <MetricCard
          label="Cycle"
          value={shortCycle(taxes.data?.cycle_id ?? cycleId ?? null)}
          footnote={latestCycle.data?.cycle_completed_at_utc ? formatDhakaDateTime(latestCycle.data.cycle_completed_at_utc) : "No cycle loaded"}
        />
        <MetricCard label="Tax rows" value={rows.length.toLocaleString()} footnote={`Limit ${limit.toLocaleString()}`} />
        <MetricCard label="Routes" value={routeCount.toLocaleString()} footnote={`${airlineCount.toLocaleString()} airlines in view`} />
        <MetricCard label="Highest tax" value={formatMoney(maxTax, "BDT")} footnote="Current filtered result set" />
      </div>

      <div className="stack">
        <DataPanel
          title="Tax filters"
          copy="Use quick route and airline chips or pin an exact origin-destination pair below. Chip clicks update the table immediately."
        >
          <LiveFilterControls
            airlineOptions={airlineOptions}
            clearKeys={["airline", "origin", "destination", "limit"]}
            initialValues={{
              origin: origin ?? "",
              destination: destination ?? "",
              limit: String(limit)
            }}
            manualFields={[
              { name: "origin", label: "Origin", placeholder: "DAC" },
              { name: "destination", label: "Destination", placeholder: "DOH" },
              { name: "limit", label: "Row limit", inputMode: "numeric", pattern: "[0-9]*" }
            ]}
            routeOptions={routeOptions}
            selectedAirlines={selectedAirlines}
            selectedRouteKey={routeKey}
          />
        </DataPanel>

        <DataPanel
          title="Tax rows"
          copy={routeKey ? `Showing tax rows for ${routeKey}.` : "Showing tax rows across the selected operational scope."}
        >
          {!taxes.ok ? (
            <div className="empty-state error-state">API error: {taxes.error ?? "Unable to load taxes."}</div>
          ) : rows.length === 0 ? (
            <div className="empty-state">No tax rows matched the current filter set.</div>
          ) : (
            <div className="data-table-wrap">
              <table className="data-table">
                <thead>
                  <tr>
                    <th>Route</th>
                    <th>Airline</th>
                    <th>Flight</th>
                    <th>Departure</th>
                    <th>Cabin</th>
                    <th>Fare basis</th>
                    <th>Tax amount</th>
                    <th>Currency</th>
                    <th>Captured</th>
                  </tr>
                </thead>
                <tbody>
                  {rows.map((row, index) => (
                    <tr
                      key={`${row.route_key}-${row.airline}-${row.flight_number}-${row.departure_utc}-${row.fare_basis ?? ""}-${row.captured_at_utc ?? ""}-${index}`}
                    >
                      <td>{row.route_key}</td>
                      <td>{row.airline}</td>
                      <td>{row.flight_number}</td>
                      <td>{formatDhakaDateTime(row.departure_utc)}</td>
                      <td>{row.cabin ?? "-"}</td>
                      <td>{row.fare_basis ?? "-"}</td>
                      <td>{formatMoney(row.tax_amount, row.currency ?? "BDT")}</td>
                      <td>{row.currency ?? "-"}</td>
                      <td>{formatDhakaDateTime(row.captured_at_utc)}</td>
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
