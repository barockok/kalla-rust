/**
 * @jest-environment node
 */

jest.mock("uuid", () => ({
  v4: () => "abcd1234-5678-9abc-def0-123456789abc",
}));

jest.mock("@/lib/s3-client", () => ({
  getObject: jest.fn(),
  UPLOADS_BUCKET: "test-bucket",
}));

jest.mock("@/lib/db", () => ({
  __esModule: true,
  default: {
    query: jest.fn(),
  },
}));

import { POST } from "@/app/api/sources/register-csv/route";
import { getObject } from "@/lib/s3-client";
import pool from "@/lib/db";

const mockGetObject = getObject as jest.Mock;
const mockQuery = pool.query as jest.Mock;

function makeRequest(body: Record<string, unknown>) {
  return new Request("http://localhost/api/sources/register-csv", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

describe("POST /api/sources/register-csv", () => {
  beforeEach(() => jest.clearAllMocks());

  test("returns 400 if s3_uri missing", async () => {
    const res = await POST(makeRequest({}));
    expect(res.status).toBe(400);
  });

  test("returns 400 if original_alias missing", async () => {
    const res = await POST(makeRequest({ s3_uri: "s3://test-bucket/key" }));
    expect(res.status).toBe(400);
  });

  test("registers CSV source and returns alias + metadata", async () => {
    const csvContent = "id,name,amount\n1,Alice,100\n2,Bob,200\n3,Charlie,300\n";
    const encoder = new TextEncoder();
    const stream = new ReadableStream({
      start(controller) {
        controller.enqueue(encoder.encode(csvContent));
        controller.close();
      },
    });
    mockGetObject.mockResolvedValue(stream);
    mockQuery.mockResolvedValue({ rows: [] });

    const res = await POST(
      makeRequest({
        s3_uri: "s3://test-bucket/session/upload/payments.csv",
        original_alias: "payments",
      }),
    );

    expect(res.status).toBe(200);
    const data = await res.json();
    expect(data.alias).toBe("csv_payments_abcd1234");
    expect(data.row_count).toBe(3);
    expect(data.col_count).toBe(3);
    expect(data.columns).toEqual(["id", "name", "amount"]);

    // Verify DB insert was called with correct params
    expect(mockQuery).toHaveBeenCalledWith(
      expect.stringContaining("INSERT INTO sources"),
      [
        "csv_payments_abcd1234",
        "s3://test-bucket/session/upload/payments.csv",
        "csv",
        "active",
      ],
    );
  });
});
