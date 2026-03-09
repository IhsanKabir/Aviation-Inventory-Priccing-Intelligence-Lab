# Round-Trip Architecture

Last updated: 2026-03-09

## Objective

Add round-trip support without breaking the existing one-way fact model.

## Decision

One-way flight observations remain the canonical fact layer. Round-trip support is added as search-intent and itinerary-link metadata around those rows.

## Current Delivery

This first architecture pass adds:

- shared trip request normalization in [`core/trip_context.py`](../core/trip_context.py)
- `run_all.py` CLI support for `--trip-type` and `--return-date`
- persisted trip metadata in `flight_offer_raw_meta`
- backward-compatible connector kwargs for round-trip adoption
- first live connector path in [`modules/biman.py`](../modules/biman.py)

The current reporting/UI pass adds:

- route-monitor API exposure for trip metadata
- route-page filters for `OW` / `RT` plus return date
- grouped outbound/inbound route shells in the web monitor

## Data Model

Round-trip context is stored in raw meta, not in the core `flight_offers` identity table.

Fields:

- `search_trip_type`: `OW` or `RT`
- `trip_request_id`: stable request fingerprint for outbound/inbound pairing
- `requested_outbound_date`
- `requested_return_date`
- `trip_duration_days`
- `trip_origin`
- `trip_destination`
- `leg_direction`: `outbound` or `inbound`
- `leg_sequence`: `1` or `2`
- `itinerary_leg_count`

## Why Raw Meta First

- the current comparison engine is built around one-way snapshot identity
- forcing round-trip identity into `flight_offers` now would destabilize current change detection
- raw meta preserves trip intent immediately while keeping the current route/flight comparison model intact

## Connector Adoption Rule

Connectors may adopt round-trip support in three levels:

1. Accept kwargs and ignore them
2. Accept kwargs and preserve trip metadata on returned rows
3. Perform true round-trip search and emit outbound/inbound rows with `leg_direction`

Current state:

- `Biman`: level 3 payload support through two `itineraryParts`
- other connectors: backward-compatible through `run_all.py` keyword fallback, but still effectively one-way until explicitly upgraded

## Web/API Implications

Implemented now:

- trip-type filter in search/reporting contracts
- route-monitor payload metadata for paired trip display
- grouped outbound/inbound route shells on Routes

Still next:

- itinerary-level export tabs
- itinerary-level ranking and pricing comparison
- warehouse-side round-trip exposure for hosted-only reads

## Next Implementation Steps

1. Upgrade OTA connectors with explicit round-trip payload builders.
2. Upgrade OTA connectors with explicit round-trip payload builders.
3. Add itinerary-level export and ranking views.
4. Add itinerary-level ranking and forecasting on top of linked legs.
