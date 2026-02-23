import { NextResponse } from "next/server";
import { v4 as uuidv4 } from "uuid";
import { getObject, UPLOADS_BUCKET } from "@/lib/s3-client";
import { parse } from "csv-parse/sync";
import pool from "@/lib/db";

export async function POST(request: Request) {
  let body: { s3_uri?: string; original_alias?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 },
    );
  }

  const { s3_uri, original_alias } = body;

  if (!s3_uri) {
    return NextResponse.json(
      { error: "Missing required field: s3_uri" },
      { status: 400 },
    );
  }

  if (!original_alias) {
    return NextResponse.json(
      { error: "Missing required field: original_alias" },
      { status: 400 },
    );
  }

  // Extract key from s3_uri: s3://bucket/key -> key
  const prefix = `s3://${UPLOADS_BUCKET}/`;
  if (!s3_uri.startsWith(prefix)) {
    return NextResponse.json(
      { error: `Invalid s3_uri: must start with ${prefix}` },
      { status: 400 },
    );
  }

  const key = s3_uri.slice(prefix.length);

  try {
    const stream = await getObject(key);
    if (!stream) {
      return NextResponse.json(
        { error: "File not found" },
        { status: 404 },
      );
    }

    // Read the stream into a string
    const reader = stream.getReader();
    const chunks: Uint8Array[] = [];
    for (;;) {
      const { value, done } = await reader.read();
      if (value) chunks.push(value);
      if (done) break;
    }

    // Concatenate chunks into a single buffer
    const totalLength = chunks.reduce((acc, c) => acc + c.length, 0);
    const buffer = new Uint8Array(totalLength);
    let offset = 0;
    for (const chunk of chunks) {
      buffer.set(chunk, offset);
      offset += chunk.length;
    }

    const csvText = new TextDecoder().decode(buffer);

    // Parse CSV with columns: true to get objects
    const records: Record<string, string>[] = parse(csvText, {
      columns: true,
      skip_empty_lines: true,
    });

    const columns = records.length > 0 ? Object.keys(records[0]) : [];
    const row_count = records.length;
    const col_count = columns.length;

    // Generate disposable alias
    const alias = `csv_${original_alias}_${uuidv4().slice(0, 8)}`;

    // Insert into sources table
    await pool.query(
      `INSERT INTO sources (alias, uri, source_type, status)
       VALUES ($1, $2, $3, $4)`,
      [alias, s3_uri, "csv", "active"],
    );

    return NextResponse.json({ alias, columns, row_count, col_count });
  } catch (err) {
    console.error("Register CSV source error:", err);
    return NextResponse.json(
      { error: "Failed to register CSV source" },
      { status: 500 },
    );
  }
}
