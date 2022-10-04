const fs = require("fs");
const { S3 } = require("aws-sdk");

// Firehose Data Transformation
const firehoseDataTransformationCompleted = "Ok";

// Access to Amazon S3
const ACCESS_KEY_ID = process.env.ACCESS_KEY_ID;
const SECRET_ACCESS_KEY = process.env.SECRET_ACCESS_KEY;
const BUCKET_NAME = process.env.BUCKET_NAME;

const s3 = new S3({
  accessKeyId: ACCESS_KEY_ID,
  secretAccessKey: SECRET_ACCESS_KEY,
});

exports.handler = async (event) => {
  if (!event.records) return {};
  if (event.records.length <= 0) return {};

  // TODO: replace the column names for your csv file
  let csvText = "ITEM_ID,USER_ID,PET_TYPE,PET_SIZE,EVENT_TYPE,TIMESTAMP\r\n";
  for (let i = 0; i < event.records.length; i++) {
    let buffer = Buffer.from(event.records[i].data, "base64");
    let json = JSON.parse(buffer.toString("ascii"));
    // TODO: replace the CSV itens for your csv inputs
    csvText += `${json.ITEM_ID},${json.USER_ID},${json.PET_TYPE},${json.PET_SIZE},${json.EVENT_TYPE},${json.TIMESTAMP}\r\n`;

    event.records[i].result = firehoseDataTransformationCompleted;
  }

  await fs.promises.writeFile("/tmp/output.csv", csvText);
  const data = await fs.promises.readFile("/tmp/output.csv");
  const date = new Date();
  const res = await s3
    .upload({
      Bucket: BUCKET_NAME,
      Key: `${date.getFullYear()}/${date.getMonth()}/${date.getDay()}/${date.toISOString()}-${
        event.invocationId
      }.csv`,
      Body: data,
    })
    .promise();

  console.log(`S3 = ${JSON.stringify(res)}`);

  return event;
};
