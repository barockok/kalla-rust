import {
  S3Client,
  GetObjectCommand,
  ListObjectsV2Command,
  DeleteObjectsCommand,
  PutObjectCommand,
} from "@aws-sdk/client-s3";
import { getSignedUrl } from "@aws-sdk/s3-request-presigner";

export const s3 = new S3Client({
  endpoint: process.env.S3_ENDPOINT ?? "http://localhost:9000",
  credentials: {
    accessKeyId: process.env.S3_ACCESS_KEY ?? "minioadmin",
    secretAccessKey: process.env.S3_SECRET_KEY ?? "minioadmin",
  },
  forcePathStyle: true,
  region: process.env.S3_REGION ?? "us-east-1",
});

export const UPLOADS_BUCKET =
  process.env.S3_UPLOADS_BUCKET ?? "kalla-uploads";

/**
 * Create a presigned PUT URL for uploading an object directly to S3.
 * The URL expires after 1 hour (3600 seconds).
 */
export async function createPresignedUploadUrl(key: string): Promise<string> {
  const command = new PutObjectCommand({
    Bucket: UPLOADS_BUCKET,
    Key: key,
  });
  return getSignedUrl(s3, command, { expiresIn: 3600 });
}

/**
 * Get an object from the uploads bucket as a WebStream (ReadableStream).
 */
export async function getObject(
  key: string
): Promise<ReadableStream | undefined> {
  const command = new GetObjectCommand({
    Bucket: UPLOADS_BUCKET,
    Key: key,
  });
  try {
    const response = await s3.send(command);
    return response.Body?.transformToWebStream();
  } catch (error: any) {
    if (error.name === "NoSuchKey") {
      return undefined;
    }
    throw error;
  }
}

/**
 * Delete all objects under a session prefix (`{sessionId}/`).
 */
export async function deleteSessionFiles(sessionId: string): Promise<void> {
  const prefix = `${sessionId}/`;
  let continuationToken: string | undefined;

  do {
    const listCommand = new ListObjectsV2Command({
      Bucket: UPLOADS_BUCKET,
      Prefix: prefix,
      ContinuationToken: continuationToken,
    });
    const listed = await s3.send(listCommand);

    if (listed.Contents && listed.Contents.length > 0) {
      const deleteCommand = new DeleteObjectsCommand({
        Bucket: UPLOADS_BUCKET,
        Delete: {
          Objects: listed.Contents.map((obj) => ({ Key: obj.Key })),
        },
      });
      await s3.send(deleteCommand);
    }

    continuationToken = listed.NextContinuationToken;
  } while (continuationToken);
}
