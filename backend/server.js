require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const Papa = require('papaparse');
const XLSX = require('xlsx');
const AWS = require('aws-sdk');
const fs = require('fs');
const path = require('path');

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

const port = process.env.PORT || 8301;
const app = express();

const corsOptions = {
  origin: 'http://localhost:5174',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
});

const dynamoDb = new AWS.DynamoDB.DocumentClient({
  maxRetries: 50,
});

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

console.log('TABLE_NAME:', TABLE_NAME);

app.post('/upload', upload.single('file'), async (req, res) => {
  console.log('File upload initiated.');
  const file = req.file;

  if (!file) {
    console.error('No file uploaded.');
    return res.status(400).json({ message: 'No file uploaded.' });
  }

  const extension = path.extname(file.originalname).toLowerCase();
  console.log(`File extension detected: ${extension}`);
  let processedData = [];

  try {
    if (extension === '.csv') {
      console.log('Processing CSV file...');
      Papa.parse(file.buffer.toString(), {
        complete: async (result) => {
          console.log('CSV parsing complete.');
          processedData = extractUniqueValues(result.data);
          console.log(`Extracted unique values: ${processedData.length}`);
          if (processedData.length === 0) {
            return res.status(200).json({ message: 'No data found in the file.', file: null });
          }
          await processAndRespond(processedData, 'csv', res);
        },
        header: false,
      });
    } else if (['.xlsx', '.xls'].includes(extension)) {
      console.log('Processing Excel file...');
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
      console.log('Excel parsing complete.');
      processedData = extractUniqueValues(jsonData);
      console.log(`Extracted unique values: ${processedData.length}`);

      if (processedData.length === 0) {
        return res.status(200).json({ message: 'No data found in the file.', file: null });
      }
      await processAndRespond(processedData, 'excel', res);
    } else {
      console.error('Invalid file type.');
      return res.status(400).json({ message: 'Invalid file type.' });
    }
  } catch (err) {
    console.error('Error processing file:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
});

// Helper function to extract unique values from all columns
const extractUniqueValues = (data) => {
  console.log('Extracting unique values from data...');
  const uniqueValues = new Set();
  data.forEach(row => {
    row.forEach(value => {
      if (value && !isNaN(value)) {
        uniqueValues.add(parseFloat(value));
      }
    });
  });
  console.log(`Unique values extraction complete. Count: ${uniqueValues.size}`);
  return Array.from(uniqueValues);
};

// Processing and response logic
const processAndRespond = async (processedData, fileType, res) => {
  try {
    console.log('Fetching existing IDs from DynamoDB...');
    let existingIds;

    try {
      const existingIdsArray = await getExistingIds(processedData);
      existingIds = new Set(existingIdsArray);
      console.log(`Fetched existing IDs. Count: ${existingIds.size}`);
    } catch (err) {
      console.error('Error in getting existing ids:', err);
      return res.status(500).json({ message: 'Error fetching existing IDs.' });
    }

    console.log('Filtering new values...');
    let newValues;
    try {
      newValues = processedData.filter(value => !existingIds.has(value));
      console.log(`New values identified. Count: ${newValues.length}`);
    } catch (err) {
      console.error('Error in comparison:', err);
      return res.status(500).json({ message: 'Error during comparison.' });
    }

    if (newValues.length === 0) {
      console.warn('All entries were duplicates.');
      return res.status(400).json({ message: 'All entries were duplicates.' });
    }

    console.log('Writing new values to DynamoDB...');
    try {
      await batchWriteNewEntries(newValues);
      console.log('New values successfully written to DynamoDB.');
    } catch (err) {
      console.error('Error in writing new values:', err);
      return res.status(500).json({ message: 'Error writing new entries.' });
    }

    console.log('Creating output file...');
    const filePath = await createFile(newValues, fileType);
    console.log(`Output file created at: ${filePath}`);
    return res.status(200).json({ message: 'File processed successfully.', file: filePath });
  } catch (err) {
    console.error('Error during processing:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
};
// Fetch existing IDs from DynamoDB with concurrency
const getExistingIds = async (processedData) => {
  console.log('Chunking processed data for batch fetching...');
  const idsChunks = chunkArray(processedData, 100);
  console.log(`Total chunks created: ${idsChunks.length}`);
  const allExistingIds = new Set();

  try {
    await Promise.all(
      idsChunks.map(async (chunk, index) => {
        console.log(`Fetching batch ${index + 1} of ${idsChunks.length}`);
        const params = {
          RequestItems: {
            [TABLE_NAME]: {
              Keys: chunk.map((id) => ({ ID: id })),
              ProjectionExpression: 'ID',
            },
          },
        };

        try {
          const result = await dynamoDb.batchGet(params).promise();
          const existingIds = result.Responses[TABLE_NAME] || [];
          //console.log(`Batch ${index + 1}: Fetched ${existingIds.length} existing IDs.`);
          existingIds.forEach((item) => allExistingIds.add(item.ID));
        } catch (err) {
          console.error(`Error fetching batch ${index + 1}:`, err);
        }
      })
    );
  } catch (err) {
    console.error('Error processing batches concurrently:', err);
  }

  console.log(`Total existing IDs found: ${allExistingIds.size}`);
  return Array.from(allExistingIds);
};

// Write new entries to DynamoDB with concurrency
const batchWriteNewEntries = async (newValues) => {
  console.log('Chunking new values for batch writing...');
  const putRequests = newValues.map((value) => ({
    PutRequest: {
      Item: { ID: value },
    },
  }));

  const chunks = chunkArray(putRequests, 25);
  console.log(`Total chunks to write: ${chunks.length}`);

  try {
    await Promise.all(
      chunks.map(async (chunk, index) => {
        const params = {
          RequestItems: {
            [TABLE_NAME]: chunk,
          },
        };

        try {
          console.log(`Writing batch ${index + 1} of ${chunks.length}`);
          await dynamoDb.batchWrite(params).promise();
          console.log(`Batch ${index + 1} written successfully.`);
        } catch (err) {
          console.error(`Error writing batch ${index + 1}:`, err);
        }
      })
    );
  } catch (err) {
    console.error('Error writing batches concurrently:', err);
  }
};

// Create output file concurrently
const createFile = async (data, fileType) => {
  console.log('Creating file with chunked data...');
  const uploadDir = path.join(__dirname, 'uploads');
  if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

  const fileName = `processed_file.${fileType}`;
  const filePath = path.join(uploadDir, fileName);

  try {
    // Chunk data into columns of 500,000 entries each
    const chunkSize = 500000;
    const chunkedData = [];
    for (let i = 0; i < data.length; i += chunkSize) {
      chunkedData.push(data.slice(i, i + chunkSize));
    }

    if (fileType === 'csv') {
      // Prepare data for CSV: Each chunk becomes a column
      const rows = [];
      const maxRows = Math.max(...chunkedData.map((chunk) => chunk.length));
      for (let rowIndex = 0; rowIndex < maxRows; rowIndex++) {
        const row = chunkedData.map((chunk) => chunk[rowIndex] || '');
        rows.push(row);
      }

      const csvContent = Papa.unparse(rows);
      await fs.promises.writeFile(filePath, csvContent);
    } else {
      // Prepare data for Excel: Each chunk becomes a column
      const sheetData = [];
      const maxRows = Math.max(...chunkedData.map((chunk) => chunk.length));
      for (let rowIndex = 0; rowIndex < maxRows; rowIndex++) {
        const row = chunkedData.map((chunk) => chunk[rowIndex] || null);
        sheetData.push(row);
      }

      const sheet = XLSX.utils.aoa_to_sheet(sheetData);
      const workbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(workbook, sheet, 'Data');
      XLSX.writeFile(workbook, filePath);
    }

    console.log(`File created at: ${filePath}`);
    return `/uploads/${fileName}`;
  } catch (err) {
    console.error('Error creating file:', err);
    throw err;
  }
};



// Utility: Split array into chunks
const chunkArray = (array, chunkSize) => {
  const chunks = [];
  for (let i = 0; i < array.length; i += chunkSize) {
    chunks.push(array.slice(i, i + chunkSize));
  }
  return chunks;
};

const server = app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on http://0.0.0.0:${port}`);
});

server.setTimeout(6000000);

module.exports = app;
