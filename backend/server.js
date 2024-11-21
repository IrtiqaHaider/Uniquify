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

const app = express();
const port = 5000;

app.use(cors({ origin: 'https://uniquify-uqvj.onrender.com/' })); 
app.use(express.json());
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// Set up AWS DynamoDB SDK configuration
AWS.config.update({
  accessKeyId: process.env.AWS_ACCESS_KEY_ID,
  secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
  region: process.env.AWS_REGION
});

const TABLE_NAME = process.env.DYNAMODB_TABLE_NAME;

// const dynamoDb = new AWS.DynamoDB();

// const params = {
//   TableName:  TABLE_NAME // Replace with your actual table name
// };

// dynamoDb.describeTable(params, (err, data) => {
//   if (err) {
//     console.error("Error connecting to DynamoDB:", err);
//   } else {
//     console.log("Successfully connected to DynamoDB! Table details:", data);
//   }
// });



const dynamoDb = new AWS.DynamoDB.DocumentClient(); // DynamoDB Document Client

// Route to handle file upload and data processing
app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;

  if (!file) {
    return res.status(400).json({ message: 'No file uploaded.' });
  }

  const extension = path.extname(file.originalname).toLowerCase();
  let processedData = [];

  try {
    // Process CSV
    if (extension === '.csv') {
      Papa.parse(file.buffer.toString(), {
        complete: async result => {
          processedData = Array.from(
            new Set(result.data.map(row => parseFloat(row[0]))),
          ).filter(Boolean);

          if (processedData.length === 0) {
            return res.status(200).json({ message: 'No data found in the file.', file: null });
          }

          await processAndRespond(processedData, 'csv', res);
        },
        header: false,
      });
    }
    // Process Excel
    else if (extension === '.xlsx' || extension === '.xls') {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });

      processedData = Array.from(
        new Set(jsonData.map(row => parseFloat(row[0]))),
      ).filter(Boolean);

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

// Process and respond based on processed data
const processAndRespond = async (processedData, fileType, res) => {
  const params = {
    TableName: TABLE_NAME, // Replace with your DynamoDB table name
    FilterExpression: '#val IN (:processedData)',
    ExpressionAttributeNames: {
      '#val': 'value', // Replace 'value' with the expression attribute name '#val'
    },
    ExpressionAttributeValues: {
      ':processedData': processedData, // This will be the list of processed values
    },
  };

  console.log('Table Name: ' , TABLE_NAME)
  console.log('Processed Data:', processedData);
  if (!Array.isArray(processedData) || processedData.length === 0) {
  console.error('Processed Data is either not an array or empty.');
  return res.status(400).json({ message: 'Invalid processed data provided.', file: null });
}


  try {
    // Query DynamoDB
    //const existingIds = await dynamoDb.scan(params).promise();  // Use scan for filter expressions
    
    // let existingIds 
    // try {
    //   existingIds = await dynamoDb.scan(params).promise();
    //   console.log('Scan Result:', existingIds);
    // } catch (err) {
    //   console.error('Error during scan:', err);
    // }

    let existingIds;
    const params = {
      TableName: TABLE_NAME,
      ProjectionExpression: 'ID', // Only fetch the 'ID' attribute
    };
  
    try {
      console.log('Scanning DynamoDB...');
      existingIds = await dynamoDb.scan(params).promise();
      //console.log('Scan Result:', JSON.stringify(existingIds, null, 2));
    } catch (err) {
      console.error('Error during scan:', err);
      return;
    }

    const existingValues = existingIds.Items.map(item => item.ID);
    console.log('Extracted IDs:', existingValues);
  
    const newValues = processedData.filter(value => !existingValues.includes(value));
    console.log('New Values to Insert:', newValues);
    
    // const existingValues = new Set(existingIds.Items.map(item => item.value));
    // const newValues = processedData.filter(id => !existingValues.has(id));

    let message;

    if (processedData.length > 0 && newValues.length === 0) {
      message = 'All entries were duplicates.';
    } else if (newValues.length === processedData.length) {
      message = 'No duplicate entries found.';
    } else {
      message = 'File processed successfully.';
    }

    console.log('Message: ', message)

    console.log('New Values: ', newValues)

    if (newValues.length > 0) {
      const putRequests = newValues.map(value => ({
        PutRequest: {
          Item: { ID: value }, // Replace "ID" with your partition key name
        },
      }));
    
      try {
        console.log('Batch Write Requests:', JSON.stringify(putRequests, null, 2));
    
        await dynamoDb.batchWrite({
          RequestItems: {
            [TABLE_NAME]: putRequests, // Ensure TABLE_NAME matches your table's name
          },
        }).promise();
    
        console.log('Batch write successful!');
      } catch (err) {
        console.error('Error during batchWrite:', err);
        throw err; // Re-throw the error to log and debug
      }
    }

    const filteredData = processedData.filter(id => !existingValues.includes(id));

    console.log('Filtered Data: ' , filteredData)

    try {
      const uploadDir = path.join(__dirname, 'uploads');
      
      // Check if 'uploads' directory exists, if not create it
      if (!fs.existsSync(uploadDir)) {
          fs.mkdirSync(uploadDir, { recursive: true }); // Use recursive to create all missing parent directories
      }
  
      if (fileType === 'csv') {
          console.log('its csv');
          const newCsvFile = Papa.unparse(filteredData.map(id => [id]));
          
          // Correct file path
          const filePath = path.join(uploadDir, 'processed_file.csv');
          
          // Write the CSV file
          fs.writeFileSync(filePath, newCsvFile);
          console.log('Write file Sync');
          console.log('Returning ...');
          
          // Return response with the path to the file
          res.status(200).json({ message, file: `/uploads/processed_file.csv` });
      } else if (fileType === 'excel') {
          console.log('its excel');
          
          const newSheet = XLSX.utils.aoa_to_sheet(filteredData.map(id => [id]));
          const newWorkbook = XLSX.utils.book_new();
          XLSX.utils.book_append_sheet(newWorkbook, newSheet, 'ProcessedData');
          const newExcelFile = XLSX.write(newWorkbook, { bookType: 'xlsx', type: 'buffer' });
          
          // Correct file path
          const filePath = path.join(uploadDir, 'processed_file.xlsx');
          
          // Write the Excel file
          fs.writeFileSync(filePath, newExcelFile);
          console.log('Write file Sync');
          console.log('Returning ...');
          
          // Return response with the path to the file
          res.status(200).json({ message, file: `/uploads/processed_file.xlsx` });
      }
  } catch (error) {
      console.error('Error writing file:', error);
      res.status(500).json({ message: 'Error writing the file.' });
  }
  
  } catch (err) {
    console.error('Error querying DynamoDB:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
};


app.listen(port, () => {
  console.log(`Server running on http://localhost:${port}`);
});