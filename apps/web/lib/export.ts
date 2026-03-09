import { getApiBaseUrl } from "@/lib/api";
import type { RawSearchParams } from "@/lib/query";

export function buildReportingExportUrl(
  params: RawSearchParams,
  include: Array<"routes" | "operations" | "changes" | "taxes" | "penalties">
) {
  const query = new URLSearchParams();

  for (const [key, value] of Object.entries(params)) {
    if (key === "window") {
      continue;
    }

    if (Array.isArray(value)) {
      for (const item of value) {
        if (item && item.trim()) {
          query.append(key, item);
        }
      }
      continue;
    }

    if (value && value.trim()) {
      query.set(key, value);
    }
  }

  for (const section of include) {
    query.append("include", section);
  }

  const queryString = query.toString();
  return `${getApiBaseUrl()}/api/v1/reporting/export.xlsx${queryString ? `?${queryString}` : ""}`;
}
