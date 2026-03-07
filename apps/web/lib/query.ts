export type RawSearchParams = Record<string, string | string[] | undefined>;

function cloneParams(params: RawSearchParams): RawSearchParams {
  const next: RawSearchParams = {};
  for (const [key, value] of Object.entries(params)) {
    next[key] = Array.isArray(value) ? [...value] : value;
  }
  return next;
}

export function firstParam(params: RawSearchParams, key: string): string | undefined {
  const value = params[key];
  if (Array.isArray(value)) {
    return value.find((item) => item && item.trim());
  }
  if (!value || !value.trim()) {
    return undefined;
  }
  return value;
}

export function manyParams(params: RawSearchParams, key: string): string[] {
  const value = params[key];
  if (Array.isArray(value)) {
    return value.filter((item) => item && item.trim());
  }
  if (!value || !value.trim()) {
    return [];
  }
  return [value];
}

export function parseLimit(value: string | undefined, fallback: number): number {
  const parsed = Number.parseInt(value ?? "", 10);
  if (!Number.isFinite(parsed) || parsed < 1) {
    return fallback;
  }
  return parsed;
}

export function buildHref(params: RawSearchParams): string {
  const search = new URLSearchParams();
  for (const [key, value] of Object.entries(params)) {
    if (Array.isArray(value)) {
      for (const item of value) {
        if (item && item.trim()) {
          search.append(key, item);
        }
      }
      continue;
    }

    if (value && value.trim()) {
      search.set(key, value);
    }
  }

  const query = search.toString();
  return query ? `?${query}` : "?";
}

export function setParam(
  params: RawSearchParams,
  key: string,
  value: string | undefined
): RawSearchParams {
  const next = cloneParams(params);
  if (!value || !value.trim()) {
    delete next[key];
    return next;
  }
  next[key] = value;
  return next;
}

export function toggleMultiParam(
  params: RawSearchParams,
  key: string,
  value: string
): RawSearchParams {
  const next = cloneParams(params);
  const current = manyParams(next, key);
  if (current.includes(value)) {
    const filtered = current.filter((item) => item !== value);
    if (filtered.length) {
      next[key] = filtered;
    } else {
      delete next[key];
    }
    return next;
  }

  next[key] = [...current, value];
  return next;
}

export function removeParams(params: RawSearchParams, keys: string[]): RawSearchParams {
  const next = cloneParams(params);
  for (const key of keys) {
    delete next[key];
  }
  return next;
}
