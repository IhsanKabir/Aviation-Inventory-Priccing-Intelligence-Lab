"use client";

import { usePathname, useRouter, useSearchParams } from "next/navigation";
import { useEffect, useMemo, useState, useTransition } from "react";

export type RouteToggleOption = {
  routeKey: string;
  origin: string;
  destination: string;
};

export type TextFieldConfig = {
  name: string;
  label: string;
  placeholder?: string;
  type?: "text" | "date";
  inputMode?: "text" | "numeric";
  pattern?: string;
};

export type ExtraToggleGroup = {
  key: string;
  label: string;
  options: Array<{ label: string; value: string }>;
  selected: string[];
  multi?: boolean;
};

export function LiveFilterControls({
  routeOptions,
  airlineOptions,
  selectedRouteKey,
  selectedAirlines,
  manualFields,
  initialValues,
  clearKeys,
  extraGroups = []
}: {
  routeOptions: RouteToggleOption[];
  airlineOptions: string[];
  selectedRouteKey?: string;
  selectedAirlines: string[];
  manualFields: TextFieldConfig[];
  initialValues: Record<string, string>;
  clearKeys: string[];
  extraGroups?: ExtraToggleGroup[];
}) {
  const router = useRouter();
  const pathname = usePathname();
  const searchParams = useSearchParams();
  const [isPending, startTransition] = useTransition();
  const [values, setValues] = useState<Record<string, string>>(initialValues);

  const syncKey = useMemo(() => JSON.stringify(initialValues), [initialValues]);

  useEffect(() => {
    setValues(initialValues);
  }, [syncKey, initialValues]);

  function replaceWith(next: URLSearchParams) {
    const query = next.toString();
    startTransition(() => {
      router.replace(query ? `${pathname}?${query}` : pathname, { scroll: false });
    });
  }

  function updateQuery(mutator: (next: URLSearchParams) => void) {
    const next = new URLSearchParams(searchParams.toString());
    mutator(next);
    replaceWith(next);
  }

  function toggleAirline(code: string) {
    updateQuery((next) => {
      const current = next.getAll("airline");
      next.delete("airline");
      if (current.includes(code)) {
        for (const item of current) {
          if (item !== code) {
            next.append("airline", item);
          }
        }
        return;
      }
      for (const item of current) {
        next.append("airline", item);
      }
      next.append("airline", code);
    });
  }

  function toggleRoute(origin: string, destination: string) {
    updateQuery((next) => {
      const currentOrigin = next.get("origin");
      const currentDestination = next.get("destination");
      if (currentOrigin === origin && currentDestination === destination) {
        next.delete("origin");
        next.delete("destination");
        return;
      }
      next.set("origin", origin);
      next.set("destination", destination);
    });
  }

  function toggleExtra(groupKey: string, value: string, multi = true) {
    updateQuery((next) => {
      const current = next.getAll(groupKey);
      next.delete(groupKey);
      if (!multi) {
        if (!current.includes(value)) {
          next.append(groupKey, value);
        }
        return;
      }

      if (current.includes(value)) {
        for (const item of current) {
          if (item !== value) {
            next.append(groupKey, item);
          }
        }
        return;
      }

      for (const item of current) {
        next.append(groupKey, item);
      }
      next.append(groupKey, value);
    });
  }

  function applyForm() {
    updateQuery((next) => {
      for (const field of manualFields) {
        const value = values[field.name]?.trim() ?? "";
        if (value) {
          next.set(field.name, value);
        } else {
          next.delete(field.name);
        }
      }
    });
  }

  function clearAll() {
    updateQuery((next) => {
      for (const key of clearKeys) {
        next.delete(key);
      }
    });
  }

  return (
    <div className="filter-stack">
      {routeOptions.length ? (
        <div className="filter-group">
          <div className="filter-label">Quick routes</div>
          <div className="chip-row">
            {routeOptions.map((item) => (
              <button
                key={item.routeKey}
                className="chip"
                data-active={selectedRouteKey === item.routeKey}
                data-pending={isPending}
                onClick={() => toggleRoute(item.origin, item.destination)}
                type="button"
              >
                {item.routeKey}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {airlineOptions.length ? (
        <div className="filter-group">
          <div className="filter-label">Airlines</div>
          <div className="chip-row">
            {airlineOptions.map((code) => (
              <button
                key={code}
                className="chip"
                data-active={selectedAirlines.includes(code)}
                data-pending={isPending}
                onClick={() => toggleAirline(code)}
                type="button"
              >
                {code}
              </button>
            ))}
          </div>
        </div>
      ) : null}

      {extraGroups.map((group) => (
        <div className="filter-group" key={group.key}>
          <div className="filter-label">{group.label}</div>
          <div className="chip-row">
            {group.options.map((option) => (
              <button
                key={`${group.key}-${option.value}`}
                className="chip"
                data-active={group.selected.includes(option.value)}
                data-pending={isPending}
                onClick={() => toggleExtra(group.key, option.value, group.multi !== false)}
                type="button"
              >
                {option.label}
              </button>
            ))}
          </div>
        </div>
      ))}

      <form
        className="filter-form"
        onSubmit={(event) => {
          event.preventDefault();
          applyForm();
        }}
      >
        <div className={`field-grid${manualFields.length === 3 ? " three-up" : ""}`}>
          {manualFields.map((field) => (
            <label className="field" key={field.name}>
              <span>{field.label}</span>
              <input
                inputMode={field.inputMode}
                name={field.name}
                onChange={(event) => {
                  const nextValue = event.target.value;
                  setValues((current) => ({ ...current, [field.name]: nextValue }));
                }}
                pattern={field.pattern}
                placeholder={field.placeholder}
                type={field.type ?? "text"}
                value={values[field.name] ?? ""}
              />
            </label>
          ))}
        </div>

        <div className="button-row">
          <button className="button-link" data-pending={isPending} type="submit">
            Apply filters
          </button>
          <button className="button-link ghost" data-pending={isPending} onClick={clearAll} type="button">
            Clear filters
          </button>
        </div>
      </form>
    </div>
  );
}
