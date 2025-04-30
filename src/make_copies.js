import { SNSClient, PublishCommand } from "@aws-sdk/client-sns";

const sns = new SNSClient({
  apiVersion: "2010-03-31",
  region: process.env.PORTER_SNS_TOPIC.split(":")[3],
});

/** @typedef {import('./index.js').ExportConfig} ExportConfig */

/**
 * Sends Copy tasks to Porter to copy an object from Google Cloud Storage to
 * Porter-supported destinations.
 * @param {string} extractionType
 * @param {ExportConfig} config
 * @param {string} sourceBucketName - The name of the bucket with the exported object
 * @param {string} sourceObjectName - The name of the exported object in Google Cloud Storage
 * @param {string} fileSequenceId - 000000000000, 000000000001, etc
 */
export default async function makeCopies(
  extractionType,
  config,
  sourceBucketName,
  sourceObjectName,
  fileSequenceId,
) {
  if (config.copies?.length) {
    const credentials = config.bigQueryClient.authClient.jsonContent;

    const projectId =
      // @ts-ignore
      credentials.project_id ||
      // @ts-ignore
      credentials.audience.match(/projects\/([0-9]+)\/locations/)[1];

    const job = {
      Job: {
        Id: `${sourceBucketName}/${sourceObjectName}`,
        Source: {
          Mode: "GCP/Storage",
          ProjectId: projectId,
          ClientConfiguration: credentials,
          BucketName: sourceBucketName,
          ObjectName: sourceObjectName,
        },
        // Create a copy task for each copy definition on the input event
        Tasks: config.copies.map((c) => {
          // By default, the object key in the destination will exactly match
          // the object name of the source file in GCS
          let destinationKey = sourceObjectName;

          // If the copy included a custom format
          if (c.DestinationFormat) {
            // If the "%FILE_SEQ_ID" directive does not appear anywhere in the
            // destination format, add it to the end, because it needs to
            // appear somewhere, or all files for a query job would end up with
            // the same S3 object key. It will be replaced later, as usual.
            if (!c.DestinationFormat.includes("%FILE_SEQ_ID")) {
              // eslint-disable-next-line no-param-reassign
              c.DestinationFormat = `${c.DestinationFormat}-%FILE_SEQ_ID`;
            }

            destinationKey = c.DestinationFormat
              // Replace each format directive
              .replace(
                /%RANGE_START_ISO/g,
                config.inclusiveRangeStart.toISOString(),
              )
              .replace(
                /%RANGE_START_DATE_ISO/g,
                config.inclusiveRangeStart.toISOString().split("T")[0],
              )
              .replace(
                /%RANGE_END_ISO/g,
                config.exclusiveRangeEnd.toISOString(),
              )
              .replace(
                /%RANGE_END_DATE_ISO/g,
                config.exclusiveRangeEnd.toISOString().split("T")[0],
              )
              .replace(/%TYPE/g, extractionType)
              .replace(/%REQUEST_ID/g, config.requestId)
              .replace(/%REQUEST_TIME/g, +config.requestTime)
              .replace(/%FILE_SEQ_ID/g, fileSequenceId);
          }

          if (c.Mode === "AWS/S3") {
            console.log(
              JSON.stringify({
                Copy: {
                  Source: `gs://${sourceBucketName}/${sourceObjectName}`,
                  Destination: `s3://${c.BucketName}/${destinationKey}`,
                },
              }),
            );

            return {
              Type: "Copy",
              Mode: c.Mode,
              BucketName: c.BucketName,
              ObjectKey: destinationKey,
              ContentType: "REPLACE",
            };
          } else if (c.Mode === "GCP/GCS") {
            console.log(
              JSON.stringify({
                Copy: {
                  Source: `gs://${sourceBucketName}/${sourceObjectName}`,
                  Destination: `gs://${c.BucketName}/${destinationKey}`,
                },
              }),
            );

            return {
              Type: "Copy",
              Mode: c.Mode,
              BucketName: c.BucketName,
              ObjectKey: destinationKey,
            };
          } else {
            throw new Error("Unknown mode");
            // TODO Add more destination modes
          }
        }),
      },
    };

    console.log(JSON.stringify({ PorterJob: job }));

    await sns.send(
      new PublishCommand({
        Message: JSON.stringify(job),
        TopicArn: process.env.PORTER_SNS_TOPIC,
      }),
    );
  }
}
