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
// const deleteAllItems = require('./deleteItems')
const port = 8303;
const app = express();

 // Apply CORS middleware

app.use(express.json({ limit: '50mb' })); // Increase JSON size limit
app.use(express.urlencoded({ limit: '50mb', extended: true })); // Increase URL-encoded size limit
app.use(express.json());
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

app.use(cors({
  origin: 'https://uniquify-uqvj.onrender.com', // Exact frontend origin
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization', 'X-Requested-With', 'Accept', 'Origin'],
}));

app.options('*', cors());

// Set up AWS DynamoDB SDK configuration
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION,
  //logger: console,
  retryDelayOptions: { base: 200 }, // Exponential backoff
});

//AWS.config.logger = console;


const dynamoDb = new AWS.DynamoDB.DocumentClient({
  maxRetries: 50,
});

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

console.log('TABLE_NAME:', TABLE_NAME);

// File upload route
app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;

  console.log('--------------- In the router --------------------------')

  if (!file) {
    return res.status(400).json({ message: 'No file uploaded.' });
  }

  const extension = path.extname(file.originalname).toLowerCase();
  let processedData = [];

  try {
    // Process CSV
    if (extension === '.csv') {
      Papa.parse(file.buffer.toString(), {
        complete: async (result) => {
          processedData = Array.from(new Set(result.data.map(row => parseFloat(row[0])))).filter(Boolean);
          if (processedData.length === 0) {
            return res.status(200).json({ message: 'No data found in the file.', file: null });
          }
          await processAndRespond(processedData, 'csv', res);
        },
        header: false,
      });
    } 
    // Process Excel
    else if (['.xlsx', '.xls'].includes(extension)) {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
      processedData = Array.from(new Set(jsonData.map(row => parseFloat(row[0])))).filter(Boolean);

      if (processedData.length === 0) {
        return res.status(200).json({ message: 'No data found in the file.', file: null });
      }
      await processAndRespond(processedData, 'excel', res);
    } else {
      return res.status(400).json({ message: 'Invalid file type.' });
    }
  } catch (err) {
    console.error('Error processing file:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
});

// Processing and response logic
const processAndRespond = async (processedData, fileType, res) => {

  try{
    console.log(' ------------ Processed Data: ------------------- \n ' , processedData)

  } catch(err){
    console.error('Error in Receiving data: ' , err)
  }

  try {

    console.log('--------------- In the process & respond --------------------------')
    let existingIds
    try{
      existingIds = await getExistingIds(processedData);
    }
    catch(err){
      console.error('Error in getting exisitnig ids:', err);
    }

    console.log('\n ------------ Successfullyy fetched Exisiting Data --------------- \n')

    let newValues
    try{
    newValues = processedData.filter(value => !existingIds.includes(value));
  }catch(err){
    console.error('Error in comparison:', err);
  }

    console.log('\n New Values Length after comparison: ', newValues.length)

    if (newValues.length === 0) {
      return res.status(400).json({ message: 'All entries were duplicates.' });
    }

    console.log('\n ------------ Going to Write Exisiting Data --------------- \n')

    try{
      await batchWriteNewEntries(newValues);
    }
    catch(err){
      console.error('Error in writing new values:', err);
    }
    const filePath = await createFile(newValues, fileType);
    return res.status(200).json({ message: 'File processed successfully.', file: filePath });
  } catch (err) {
    console.error('Error during processing:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
};

// Fetch existing IDs with retry handling
// Fetch existing IDs with retry and timeout
// Modify getExistingIds to handle parallel requests
const getExistingIds = async (processedData) => {
  const idsChunks = chunkArray(processedData, 100);
  const allExistingIds = [];

  console.log('--------------- Getting Existing Records from DB --------------------------');

  try {
    const results = await Promise.all(idsChunks.map(async (chunk) => {
      const params = {
        RequestItems: {
          [TABLE_NAME]: {
            Keys: chunk.map(id => ({ ID: id })),
            ProjectionExpression: 'ID',
          },
        },
      };
      const result = await dynamoDb.batchGet(params).promise();
      return result.Responses[TABLE_NAME] || [];
    }));

    results.forEach(result => {
      allExistingIds.push(...result.map(item => item.ID));
    });

    console.log('Completed fetching all existing IDs');
  } catch (err) {
    console.error('Error processing batches:', err);
  }

  return allExistingIds;
};



// Batch write new entries with throttling handling
const batchWriteNewEntries = async (newValues) => {

  console.log('--------------- Writing New Records into DB --------------------------')

  const putRequests = newValues.map(value => ({
    PutRequest: {
      Item: { ID: value },
    },
  }));

  const chunks = chunkArray(putRequests, 25);

  let i = 0

  for (const chunk of chunks) {

    i++;

    
    console.log('------------ Processing Chunk: ', i)
    //console.log('Chunk: ', chunk)

    const params = {
      RequestItems: {
        [TABLE_NAME]: chunk,
      },
    };

    try {
      await dynamoDb.batchWrite(params).promise();
      //delay(1000)
    } catch (err) {
      if (err.retryable) {
        console.warn('Retryable error during batchWrite:', err);
        await delay(1000); // Add delay before retry
        await batchWriteNewEntries(chunk.map(request => request.PutRequest.Item.ID));
      } else {
        console.error('Error during batchWrite:', err);
        throw err;
      }
    }
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

// Utility: Delay function
const delay = (ms) => new Promise(resolve => setTimeout(resolve, ms));

// Create a file (CSV/Excel) after processing
const createFile = async (data, fileType) => {
  const uploadDir = path.join(__dirname, 'uploads');
  if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

  const fileName = `processed_file.${fileType}`;
  const filePath = path.join(uploadDir, fileName);

  if (fileType === 'csv') {
    const csvContent = Papa.unparse(data.map(id => [id]));
    fs.writeFileSync(filePath, csvContent);
  } else {
    const sheet = XLSX.utils.aoa_to_sheet(data.map(id => [id]));
    const workbook = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(workbook, sheet, 'Data');
    XLSX.writeFile(workbook, filePath);
  }

  return `/uploads/${fileName}`;
};

const server = app.listen(port, '0.0.0.0', () => {
  console.log(`Server running on http://0.0.0.0:${port}`);
});

server.setTimeout(6000000); // 10 minutes, adjust as needed


module.exports = app;