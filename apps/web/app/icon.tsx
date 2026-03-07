import { ImageResponse } from "next/og";

export const size = {
  width: 32,
  height: 32
};

export const contentType = "image/png";

export default function Icon() {
  return new ImageResponse(
    (
      <div
        style={{
          alignItems: "center",
          background: "#0f5e88",
          color: "#f5f7fb",
          display: "flex",
          fontSize: 16,
          fontWeight: 700,
          height: "100%",
          justifyContent: "center",
          width: "100%"
        }}
      >
        AP
      </div>
    ),
    size
  );
}
