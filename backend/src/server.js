const express = require('express');
const cors = require('cors');
const multer = require('multer');
const Papa = require('papaparse');
const XLSX = require('xlsx');
const mongoose = require('mongoose');
const fs = require('fs');
const path = require('path');

const app = express();
const port = 5000;

const corsOptions = {
  origin: 'http://localhost:5173', // Frontend URL
  methods: ['GET', 'POST'],
  allowedHeaders: ['Content-Type'],
};

app.use(cors(corsOptions)); // Apply CORS middleware

app.use('/uploads', express.static(path.join(__dirname, 'uploads')));


const DB_String =
  'mongodb+srv://protogroup:Proto123!@cluster0.33jcm.mongodb.net/ExcelApp?retryWrites=true&w=majority&appName=Cluster0';

// MongoDB Atlas Connection
mongoose
  .connect(DB_String, { useNewUrlParser: true, useUnifiedTopology: true })
  .then(() => console.log('Connected to MongoDB'))
  .catch(err => console.error('Error connecting to MongoDB:', err));

// Define schema and model for MongoDB collection
const DataSchema = new mongoose.Schema({
  value: Number,
});

const Data = mongoose.model('Data', DataSchema);

// Middleware to handle file uploads
const storage = multer.memoryStorage();
const upload = multer({ storage: storage });

app.use(express.json());

// Route to handle file upload and data processing
app.post('/upload', upload.single('file'), async (req, res) => {
  const file = req.file;

  console.log('In the router')
  console.log(file)

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

const processAndRespond = async (processedData, fileType, res) => {
    const existingIds = await Data.find({ value: { $in: processedData } }).lean();
    const existingValues = new Set(existingIds.map(item => item.value));
    const newValues = processedData.filter(id => !existingValues.has(id));
  
    let message;
  
    if (processedData.length > 0 && newValues.length === 0) {
      message = 'All entries were duplicates.';
    } else if (newValues.length === processedData.length) {
      message = 'No duplicate entries found.';
    } else {
      message = 'File processed successfully.';
    }
  
    // Add new values to MongoDB
    if (newValues.length > 0) {
      await Data.insertMany(newValues.map(value => ({ value })));
    }
  
    const filteredData = processedData.filter(id => !existingValues.has(id));
  
    // Handle file generation for filtered data
    if (fileType === 'csv') {
      const newCsvFile = Papa.unparse(filteredData.map(id => [id]));
      const filePath = path.join(__dirname, 'uploads', 'processed_file.csv');
      fs.writeFileSync(filePath, newCsvFile);
  
      res.status(200).json({ message, file: `/uploads/processed_file.csv` });
    } else if (fileType === 'excel') {
      const newSheet = XLSX.utils.aoa_to_sheet(filteredData.map(id => [id]));
      const newWorkbook = XLSX.utils.book_new();
      XLSX.utils.book_append_sheet(newWorkbook, newSheet, 'ProcessedData');
      const newExcelFile = XLSX.write(newWorkbook, { bookType: 'xlsx', type: 'buffer' });
  
      const filePath = path.join(__dirname, 'uploads', 'processed_file.xlsx');
      fs.writeFileSync(filePath, newExcelFile);
      
      res.status(200).json({ message, file: `/uploads/processed_file.xlsx` });
    }
  };
  
  app.listen(port, () => {
    console.log(`Server running on http://localhost:${port}`);
  });
  

