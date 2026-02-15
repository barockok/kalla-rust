import { NextResponse } from "next/server";
import { v4 as uuidv4 } from "uuid";
import { createPresignedUploadUrl, UPLOADS_BUCKET } from "@/lib/s3-client";

export async function POST(request: Request) {
  let body: { filename?: string; session_id?: string };
  try {
    body = await request.json();
  } catch {
    return NextResponse.json(
      { error: "Invalid JSON body" },
      { status: 400 }
    );
  }

  const { filename, session_id } = body;

  if (!filename || !session_id) {
    return NextResponse.json(
      { error: "Missing required fields: filename, session_id" },
      { status: 400 }
    );
  }

  if (!filename.endsWith(".csv")) {
    return NextResponse.json(
      { error: "Only .csv files are supported" },
      { status: 400 }
    );
  }

  const upload_id = uuidv4();
  const key = `${session_id}/${upload_id}/${filename}`;
  const s3_uri = `s3://${UPLOADS_BUCKET}/${key}`;

  const presigned_url = await createPresignedUploadUrl(key);

  return NextResponse.json({ upload_id, presigned_url, s3_uri });
}
