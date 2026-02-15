import { NextResponse } from "next/server";
import { parse } from "csv-parse/sync";
import { getObject, UPLOADS_BUCKET } from "@/lib/s3-client";

export async function POST(
  request: Request,
  { params }: { params: Promise<{ uploadId: string }> }
) {
  const { uploadId } = await params;

  let body: { s3_uri?: string; filename?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 }
    );
  }

  const { s3_uri, filename } = body;

  if (!s3_uri || !filename) {
    return NextResponse.json(
      { error: "Missing required fields: s3_uri, filename" },
      { status: 400 }
    );
  }

  const prefix = `s3://${UPLOADS_BUCKET}/`;
  const key = s3_uri.startsWith(prefix)
    ? s3_uri.slice(prefix.length)
    : s3_uri;

  let stream: ReadableStream | undefined;
  try {
    stream = await getObject(key);
  } catch {
    return NextResponse.json(
      { error: "Failed to read file from storage" },
      { status: 500 }
    );
  }

  if (!stream) {
    return NextResponse.json(
      { error: "File not found" },
      { status: 404 }
    );
  }

  let text: string;
  try {
    const reader = stream.getReader();
    const decoder = new TextDecoder("utf-8");
    const chunks: string[] = [];
    let done = false;

    while (!done) {
      const result = await reader.read();
      done = result.done;
      if (result.value) {
        chunks.push(decoder.decode(result.value, { stream: !done }));
      }
    }

    text = chunks.join("");
  } catch {
    return NextResponse.json(
      { error: "Failed to read file content" },
      { status: 500 }
    );
  }

  const records: string[][] = parse(text, {
    columns: false,
    skip_empty_lines: true,
  });

  if (records.length === 0) {
    return NextResponse.json(
      { error: "CSV file is empty" },
      { status: 400 }
    );
  }

  const columns = records[0];
  const row_count = records.length - 1;

  return NextResponse.json({
    upload_id: uploadId,
    filename,
    s3_uri,
    columns,
    row_count,
  });
}
