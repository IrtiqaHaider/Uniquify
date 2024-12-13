require('dotenv').config();
const express = require('express');
const cors = require('cors');
const multer = require('multer');
const Papa = require('papaparse');
const XLSX = require('xlsx');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

// Mongoose models
const { ClientData } = require('./src/schema'); // Assumes schemas from earlier are in models.js

const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

const port = process.env.PORT || 8301;
const app = express();

const corsOptions = {
  origin: 'http://localhost:5173',
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions));
app.use(express.json({ limit: '50mb' }));
app.use(express.urlencoded({ limit: '50mb', extended: true }));
app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

const mongoUri = process.env.MONGO_URI;

// Connect to MongoDB
mongoose.connect(mongoUri, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch((err) => console.error('Error connecting to MongoDB:', err));

// Periodically log memory usage
// setInterval(() => {
//   const memoryUsage = process.memoryUsage();
//   console.log('Memory Usage:', {
//     rss: memoryUsage.rss / 1024 / 1024,  // RSS memory
//     heapTotal: memoryUsage.heapTotal / 1024 / 1024,
//     heapUsed: memoryUsage.heapUsed / 1024 / 1024,
//     external: memoryUsage.external / 1024 / 1024,
//   });
// }, 10000);

app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;

  if (!file) return res.status(400).json({ message: 'No file uploaded.' });

  const extension = path.extname(file.originalname).toLowerCase();
  let processedData = [];

  try {
    if (extension === '.csv') {
      Papa.parse(file.buffer.toString(), {
        complete: async (result) => {
          processedData = extractUniqueValues(result.data);
          if (processedData.length === 0) {
            return res.status(200).json({ message: 'No data found in the file.', file: null });
          }
          await processAndRespond(processedData, file.originalname, 'csv', res);
        },
        header: false,
      });
    } else if (['.xlsx', '.xls'].includes(extension)) {
      const workbook = XLSX.read(file.buffer, { type: 'buffer' });
      const sheetName = workbook.SheetNames[0];
      const sheet = workbook.Sheets[sheetName];
      const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
      processedData = extractUniqueValues(jsonData);
      if (processedData.length === 0) {
        return res.status(200).json({ message: 'No data found in the file.', file: null });
      }
      await processAndRespond(processedData, file.originalname, 'xlsx', res);
    } else {
      return res.status(400).json({ message: 'Invalid file type.' });
    }
  } catch (err) {
    console.error('Error processing file:', err);
    return res.status(500).json({ message: 'Error processing file.', file: null });
  }
});

// Helper function to extract unique values
const extractUniqueValues = (data) => {
  const uniqueValues = new Set();
  data.forEach((row) => {
    row.forEach((value) => {
      if (value && !isNaN(value)) uniqueValues.add(parseFloat(value));
    });
  });
  return Array.from(uniqueValues);
};

// Process data and respond
const processAndRespond = async (processedData, fileName, fileType, res) => {
    try {
      // Check for existing IDs in the ClientData collection
      // const existingIds = await ClientData.find({ id: { $in: processedData } }).select('id');
      // console.log(` ------- Exisiting Values: ${existingIds.length}`)

      const BATCH_SIZE = 1000000; // Adjust batch size as needed
      let existingIds = [];
      for (let i = 0; i < processedData.length; i += BATCH_SIZE) {
          const batch = processedData.slice(i, i + BATCH_SIZE);
          const batchIds = await ClientData.find({ id: { $in: batch } }).select('id');
          existingIds = existingIds.concat(batchIds);
      }
      console.log(` ------- Existing Values: ${existingIds.length}`);


      const existingIdSet = new Set(existingIds.map((doc) => doc.id));
      console.log(` -------- Existing IDs SeT: ${existingIdSet} `)

      const newValues = processedData.filter((value) => !existingIdSet.has(value));
      const duplicateValues = processedData.filter((value) => existingIdSet.has(value));
        
      console.log(` ------- New Values: ${newValues.length}`)
      console.log(` ------- Duplicate Values: ${duplicateValues.length}`)


      // If no new values, return a message
      if (newValues.length === 0) {
        console.warn('All entries were duplicates.');

        const DuplicatesFilePath = await createFile(duplicateValues, fileType , 'Duplicate');

        const NewFilePath = null;

        console.log(`Output files created:
            New file: ${NewFilePath}
            Duplicate file: ${DuplicatesFilePath}`);

        return res.status(200).json({ message: 'All entries were duplicates.' , files: {
            new: NewFilePath,
            duplicate: DuplicatesFilePath
          }});
      }


      if (duplicateValues.length === 0) {
        console.log('All entries are new.');
      }
      

      //const BATCH_SIZE = 100000; // Adjust batch size as needed
      for (let i = 0; i < newValues.length; i += BATCH_SIZE) {
          const batch = newValues.slice(i, i + BATCH_SIZE);
          await ClientData.insertMany(batch.map((id) => ({ id })));
      }

      // Insert new entries into the 'ClientData' collection
      // await ClientData.insertMany(newValues.map((id) => ({ id })));
  
      // Create the output file
      const NewFilePath = await createFile(newValues, fileType,'New');
      const DuplicatesFilePath = await createFile(duplicateValues, fileType , 'Duplicate');
  
      // Return response with file path
      return res.status(200).json({ message: 'File processed successfully.', files: {
        new: NewFilePath,
        duplicate: DuplicatesFilePath
      }});
    } catch (err) {
      console.error('Error during processing:', err);
      return res.status(500).json({ message: 'Error processing file.', files: null });
    }
  };

// Create output file
// const createFile = async (data, fileType) => {
//   const uploadDir = path.join(__dirname, 'uploads');
//   if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);

//   const fileName = `processed_file_Mongo.${fileType}`;
//   const filePath = path.join(uploadDir, fileName);

//   // Define the number of rows to process (500,000 rows)
//   const numRows = 500000;

//   // Create an array of rows with available data
//   const rows = [];
//   for (let i = 0; i < numRows; i++) {
//     const row = data.map(column => {
//       // Check if the current column has data at the current index
//       return column[i] !== undefined ? column[i] : '';  // Use empty string if no data available
//     });
    
//     // If any column has no data at this row, stop processing
//     if (row.some(value => value === '')) break;
    
//     rows.push(row.join(','));  // Join each column entry with a comma
//   }

//   // Join all rows with a new line
//   const fileContent = rows.join('\n');

//   // Write the content to the file
//   fs.writeFileSync(filePath, fileContent);

//   return `/uploads/${fileName}`;
// };


const createFile = async (data, fileType, fileLabel) => {
    const uploadDir = path.join(__dirname, 'uploads');
    if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);
  
    const fileName = `processed_file_${fileLabel}.${fileType}`;
    const filePath = path.join(uploadDir, fileName);
  
    try {
      const chunkSize = 500000;
      const chunkedData = [];
      for (let i = 0; i < data.length; i += chunkSize) {
        chunkedData.push(data.slice(i, i + chunkSize));
      }
  
      const rows = [];
      chunkedData.forEach(chunk => {
        chunk.forEach((value, index) => {
          rows[index] = rows[index] || [];
          rows[index].push(value);
        });
      });
  
      if (fileType === 'csv') {
        const csvContent = Papa.unparse(rows);
        await fs.promises.writeFile(filePath, csvContent);
      } else {
        const sheet = XLSX.utils.aoa_to_sheet(rows);
        const workbook = XLSX.utils.book_new();
        XLSX.utils.book_append_sheet(workbook, sheet, 'Data');
        XLSX.writeFile(workbook, filePath);
      }
  
      return `/uploads/${fileName}`;
    } catch (err) {
      console.error('Error creating file:', err);
      throw err;
    }
  };
  
app.listen(port, () => {
  console.log(`Server is running on port ${port}`);
});


// require('dotenv').config();
// const express = require('express');
// const cors = require('cors');
// const multer = require('multer');
// const Papa = require('papaparse');
// const XLSX = require('xlsx');
// const { Pool } = require('pg');  // PostgreSQL client
// const fs = require('fs');
// const path = require('path');

// const storage = multer.memoryStorage();
// const upload = multer({ storage: storage });

// const port = process.env.PORT || 8301;
// const app = express();

// const corsOptions = {
//   origin: 'http://localhost:5173',
//   methods: ['GET', 'POST'],
//   allowedHeaders: ['Content-Type'],
// };

// app.use(cors(corsOptions));
// app.use(express.json({ limit: '50mb' }));
// app.use(express.urlencoded({ limit: '50mb', extended: true }));
// app.use('/uploads', express.static(path.join(__dirname, 'uploads')));

// // Create PostgreSQL pool
// const pool = new Pool({
//   user: process.env.PG_USER, // PostgreSQL username
//   host: process.env.PG_HOST, // PostgreSQL host (usually localhost)
//   database: process.env.PG_DATABASE, // Database name
//   password: process.env.PG_PASSWORD, // PostgreSQL password
//   port: process.env.PG_PORT, // PostgreSQL port
// });

// app.post('/upload', upload.single('file'), async (req, res) => {
//   const file = req.file;

//   if (!file) return res.status(400).json({ message: 'No file uploaded.' });

//   const extension = path.extname(file.originalname).toLowerCase();
//   let processedData = [];

//   try {
//     if (extension === '.csv') {
//       const parseCSV = (csvData) =>
//         new Promise((resolve, reject) => {
//           Papa.parse(csvData, {
//             complete: (result) => resolve(result.data),
//             error: (err) => reject(err),
//             header: false,
//           });
//         });
    
//       try {
//         const result = await parseCSV(file.buffer.toString());
//         processedData = extractUniqueValues(result);
//         if (processedData.length === 0) {
//           return res.status(200).json({ message: 'No data found in the file.', file: null });
//         }
//         await processAndRespond(processedData, 'csv', res);
//       } catch (err) {
//         console.error('Error parsing CSV:', err);
//         return res.status(500).json({ message: 'Error processing CSV file.', file: null });
//       }
//     }
//      else if (['.xlsx', '.xls'].includes(extension)) {
//       const workbook = XLSX.read(file.buffer, { type: 'buffer' });
//       const sheetName = workbook.SheetNames[0];
//       const sheet = workbook.Sheets[sheetName];
//       const jsonData = XLSX.utils.sheet_to_json(sheet, { header: 1 });
//       processedData = extractUniqueValues(jsonData);
//       if (processedData.length === 0) {
//         return res.status(200).json({ message: 'No data found in the file.', file: null });
//       }
//       await processAndRespond(processedData, file.originalname, 'xlsx', res);
//     } else {
//       return res.status(400).json({ message: 'Invalid file type.' });
//     }
//   } catch (err) {
//     console.error('Error processing file:', err);
//     return res.status(500).json({ message: 'Error processing file.', file: null });
//   }
// });

// // Helper function to extract unique values
// const extractUniqueValues = (data) => {
//   const uniqueValues = new Set();
//   data.forEach((row) => {
//     row.forEach((value) => {
//       if (value && !isNaN(value)) uniqueValues.add(parseFloat(value));
//     });
//   });
//   return Array.from(uniqueValues);
// };

// // Process data and respond
// const processAndRespond = async (processedData, fileType, res) => {
//   try {
//     // Convert processedData to numeric
//     const numericProcessedData = processedData.map((value) => Number(value));

//     // Query existing IDs
//     const existingIds = await pool.query(
//       'SELECT id FROM client_data WHERE id = ANY($1::numeric[])',
//       [numericProcessedData]
//     );

//     // Extract existing IDs into a Set
//     const existingIdSet = new Set(existingIds.rows.map((row) => Number(row.id)));

//     // Identify new and duplicate values
//     const newValues = numericProcessedData.filter(
//       (value) => !existingIdSet.has(value)
//     );
//     const duplicateValues = numericProcessedData.filter(
//       (value) => existingIdSet.has(value)
//     );

//     console.log(`New Values: ${newValues.length}`);
//     console.log(`Duplicate Values: ${duplicateValues.length}`);
//     if (newValues.length === 0) {
//       const DuplicatesFilePath = await createFile(duplicateValues, fileType , 'duplicate');
//       const NewFilePath = null;

//       console.log(`Output files created:
//         New file: ${NewFilePath}
//         Duplicate file: ${DuplicatesFilePath}`);

//       return res.status(200).json({
//         message: 'All entries were duplicates.',
//         files: {
//           new: NewFilePath,
//           duplicate: DuplicatesFilePath,
//         },
//       });
//     }

//     // Insert new values into the database
//     await pool.query(
//       'INSERT INTO client_data (id) VALUES ' +
//         newValues.map((value) => `(${value})`).join(',')
//     );

//     // Create output files
//     const NewFilePath = await createFile(newValues, fileType , 'new');
//     const DuplicatesFilePath = await createFile(duplicateValues, fileType , 'duplicate');

//     // Return success response
//     return res.status(200).json({
//       message: 'File processed successfully.',
//       files: {
//         new: NewFilePath,
//         duplicate: DuplicatesFilePath,
//       },
//     });
//   } catch (err) {
//     console.error('Error during processing:', err);
//     return res.status(500).json({
//       message: 'Error processing file.',
//       files: null,
//     });
//   }
// };


// const createFile = async (data, fileType, fileLabel) => {
//     const uploadDir = path.join(__dirname, 'uploads');
//     if (!fs.existsSync(uploadDir)) fs.mkdirSync(uploadDir);
  
//     const fileName = `processed_file_${fileLabel}.${fileType}`;
//     const filePath = path.join(uploadDir, fileName);
  
//     try {
//       const chunkSize = 500000;
//       const chunkedData = [];
//       for (let i = 0; i < data.length; i += chunkSize) {
//         chunkedData.push(data.slice(i, i + chunkSize));
//       }
  
//       const rows = [];
//       chunkedData.forEach(chunk => {
//         chunk.forEach((value, index) => {
//           rows[index] = rows[index] || [];
//           rows[index].push(value);
//         });
//       });
  
//       if (fileType === 'csv') {
//         const csvContent = Papa.unparse(rows);
//         await fs.promises.writeFile(filePath, csvContent);
//       } else {
//         const sheet = XLSX.utils.aoa_to_sheet(rows);
//         const workbook = XLSX.utils.book_new();
//         XLSX.utils.book_append_sheet(workbook, sheet, 'Data');
//         XLSX.writeFile(workbook, filePath);
//       }
  
//       return `/uploads/${fileName}`;
//     } catch (err) {
//       console.error('Error creating file:', err);
//       throw err;
//     }
//   };


// app.listen(port, () => {
//   console.log(`Server is running on port ${port}`);
// });
